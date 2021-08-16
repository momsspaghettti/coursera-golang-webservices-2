package main

import (
	"context"
	"encoding/json"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"log"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"
)

// тут вы пишете код
// обращаю ваше внимание - в этом задании запрещены глобальные переменные

type aclItem *regexp.Regexp

type authACL map[string][]aclItem

func parseAcl(ACLConfig string) (authACL, error) {
	var aclRaw map[string][]string
	if err := json.Unmarshal([]byte(ACLConfig), &aclRaw); err != nil || aclRaw == nil {
		return nil, err
	}

	res := authACL(make(map[string][]aclItem, len(aclRaw)))
	for k, v := range aclRaw {
		aclItems := make([]aclItem, 0, len(v))
		for _, s := range v {
			var i aclItem
			if strings.HasSuffix(s, "/*") {
				i = regexp.MustCompile("^" + s[:len(s)-1] + ".+$")
			} else {
				i = regexp.MustCompile("^" + s + "$")
			}
			aclItems = append(aclItems, i)
		}
		res[k] = aclItems
	}

	return res, nil
}

type authCheck interface {
	check(string, string) bool
}

type authCheckImpl struct {
	inner authACL
}

func (c *authCheckImpl) check(consumer, methodName string) bool {
	allowedMethods, exists := c.inner[consumer]
	if !exists || len(allowedMethods) == 0 {
		return false
	}

	for _, methodRegexp := range allowedMethods {
		if (*methodRegexp).MatchString(methodName) {
			return true
		}
	}

	return false
}

type blServer struct {
	UnimplementedBizServer
}

func (bServer *blServer) Check(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bServer *blServer) Add(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

func (bServer *blServer) Test(context.Context, *Nothing) (*Nothing, error) {
	return &Nothing{}, nil
}

type adminServerImpl struct {
	authChecker authCheck
	listen      listeners
	UnimplementedAdminServer
}

type listeners struct {
	ls []*listener
	mu sync.Mutex
}

func (ls *listeners) getNewListener() *listener {
	l := newListener()
	ls.mu.Lock()
	defer ls.mu.Unlock()
	ls.ls = append(ls.ls, l)
	return l
}

func (ls *listeners) notify(e *Event) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	for _, l := range ls.ls {
		l.dataChan <- e.Clone()
	}
}

func (ls *listeners) sendStop() {
	for _, l := range ls.ls {
		l.controlChan <- nil
		close(l.dataChan)
		close(l.controlChan)
	}
}

type listener struct {
	controlChan chan interface{}
	dataChan    chan *Event
}

func newListener() *listener {
	return &listener{
		make(chan interface{}, 1),
		make(chan *Event, 1),
	}
}

func (aServer *adminServerImpl) Logging(_ *Nothing, outStream Admin_LoggingServer) error {
	l := aServer.listen.getNewListener()
	for {
		select {
		case <-l.controlChan:
			return nil
		case e := <-l.dataChan:
			if err := outStream.Send(e); err != nil {
				return err
			}
		}
	}
}

func (aServer *adminServerImpl) Statistics(interval *StatInterval, outStream Admin_StatisticsServer) error {
	l := aServer.listen.getNewListener()
	ticker := time.NewTicker(time.Duration(interval.IntervalSeconds) * time.Second)
	stat := newStat()
	for {
		select {
		case <-l.controlChan:
			ticker.Stop()
			return nil
		case e := <-l.dataChan:
			stat.ByMethod[e.Method]++
			stat.ByConsumer[e.Consumer]++
		case <-ticker.C:
			stat.Timestamp = aServer.getTimestamp()
			if err := outStream.Send(stat); err != nil {
				return err
			}
			stat = newStat()
		}
	}
}

func newStat() *Stat {
	return &Stat{
		ByMethod:   make(map[string]uint64, 8),
		ByConsumer: make(map[string]uint64, 8),
	}
}

func (x *Event) Clone() *Event {
	return &Event{
		Timestamp: x.GetTimestamp(),
		Consumer:  x.GetConsumer(),
		Method:    x.GetMethod(),
		Host:      x.GetHost(),
	}
}

func (aServer *adminServerImpl) middlewareHandler(ctx context.Context, fullMethod string) error {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok || md == nil {
		return status.Error(codes.Unauthenticated, "bad metadata")
	}

	consumers, exists := md["consumer"]
	if !exists || len(consumers) != 1 {
		return status.Error(codes.Unauthenticated, "no consumer")
	}

	if !aServer.authChecker.check(consumers[0], fullMethod) {
		return status.Error(codes.Unauthenticated, "permissions denied")
	}

	p, ok := peer.FromContext(ctx)
	if !ok || p == nil {
		return status.Error(codes.Internal, "can not get peer")
	}

	event := Event{
		Timestamp: aServer.getTimestamp(),
		Consumer:  consumers[0],
		Method:    fullMethod,
		Host:      p.Addr.String(),
	}

	aServer.listen.notify(&event)

	return nil
}

func (aServer *adminServerImpl) middlewareUnaryHandler(
	ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler,
) (interface{}, error) {
	if err := aServer.middlewareHandler(ctx, info.FullMethod); err != nil {
		return nil, err
	}

	return handler(ctx, req)
}

func (aServer *adminServerImpl) middlewareStreamHandler(
	srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler,
) error {
	if err := aServer.middlewareHandler(ss.Context(), info.FullMethod); err != nil {
		return err
	}

	return handler(srv, ss)
}

func (aServer *adminServerImpl) getTimestamp() int64 {
	return time.Now().Unix()
}

func (aServer *adminServerImpl) Stop() {
	aServer.listen.sendStop()
}

func runServices(ctx context.Context, listenAddr string, c authCheck) {
	lis, err := net.Listen("tcp", listenAddr)

	if err != nil {
		log.Fatal(err)
	}

	bServer := blServer{}
	aServer := adminServerImpl{
		authChecker: c,
	}

	server := grpc.NewServer(
		grpc.UnaryInterceptor(aServer.middlewareUnaryHandler),
		grpc.StreamInterceptor(aServer.middlewareStreamHandler))
	RegisterBizServer(server, &bServer)
	RegisterAdminServer(server, &aServer)

	go func() {
		_ = server.Serve(lis)
	}()

	for {
		select {
		case <-ctx.Done():
			if err = lis.Close(); err != nil {
				log.Fatal(err)
			}
			server.Stop()
			aServer.Stop()
			return
		}
	}
}

func StartMyMicroservice(ctx context.Context, listenAddr, ACLConfig string) error {
	authAcl, err := parseAcl(ACLConfig)
	if err != nil {
		return err
	}

	go runServices(ctx, listenAddr, &authCheckImpl{authAcl})
	return nil
}
