package main

import (
	"flag"
	"fmt"
	"github.com/nyu-distributed-systems-fa18/starter-code-lab2/pb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"log"
	"os"
	"time"
)

func usage() {
	fmt.Printf("Usage %s <endpoint>\n", os.Args[0])
	flag.PrintDefaults()
}

func reconnect(res *pb.Result, kvc *pb.KvStoreClient) {
	endpoint := res.GetRedirect().Server
	log.Printf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	*kvc = pb.NewKvStoreClient(conn)
}

func main() {
	// Take endpoint as input
	flag.Usage = usage
	flag.Parse()
	// If there is no endpoint fail
	if flag.NArg() == 0 {
		flag.Usage()
		os.Exit(1)
	}
	endpoint := flag.Args()[0]
	log.Printf("Connecting to %v", endpoint)
	// Connect to the server. We use WithInsecure since we do not configure https in this class.
	conn, err := grpc.Dial(endpoint, grpc.WithInsecure())
	//Ensure connection did not fail.
	if err != nil {
		log.Fatalf("Failed to dial GRPC server %v", err)
	}
	log.Printf("Connected")
	// Create a KvStore client
	kvc := pb.NewKvStoreClient(conn)
	// Clear KVC
	res, err := kvc.Clear(context.Background(), &pb.Empty{})
	log.Printf("%v", res.GetRedirect())
	log.Printf("%v", err)
	for {
		if res.GetRedirect() != nil {
			//if reflect.TypeOf(res.Result).String() == "*pb.Result_Redirect" {
			reconnect(res, &kvc)
			log.Printf("reconnected")
			res, err = kvc.Clear(context.Background(), &pb.Empty{})
		} else {
			break
		}
	}
	if err != nil {
		log.Fatalf("Could not clear")
	}
	log.Printf("Succussfully")

	time.Sleep(2 * time.Second)
	// Put setting hello -> 1
	putReq := &pb.KeyValue{Key: "hello", Value: "1"}
	res, err = kvc.Set(context.Background(), putReq)
	for {
		if res.GetRedirect() != nil {
			//if reflect.TypeOf(res.Result).String() == "*pb.Result_Redirect" {
			reconnect(res, &kvc)
			log.Printf("reconnected")
			res, err = kvc.Set(context.Background(), putReq)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatalf("Put error")
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Put returned the wrong response")
	}

	time.Sleep(2 * time.Second)
	// Request value for hello
	req := &pb.Key{Key: "hello"}
	res, err = kvc.Get(context.Background(), req)
	for {
		if res.GetRedirect() != nil {
			//if reflect.TypeOf(res.Result).String() == "*pb.Result_Redirect" {
			reconnect(res, &kvc)
			log.Printf("reconnected")
			res, err = kvc.Get(context.Background(), req)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "1" {
		log.Fatalf("Get returned the wrong response")
	}

	time.Sleep(2 * time.Second)
	// Successfully CAS changing hello -> 2
	casReq := &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	for {
		if res.GetRedirect() != nil {
			//if reflect.TypeOf(res.Result).String() == "*pb.Result_Redirect" {
			reconnect(res, &kvc)
			log.Printf("reconnected")
			res, err = kvc.CAS(context.Background(), casReq)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value != "2" {
		log.Fatalf("Get returned the wrong response")
	}

	time.Sleep(2 * time.Second)
	// Unsuccessfully CAS
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hello", Value: "1"}, Value: &pb.Value{Value: "3"}}
	res, err = kvc.CAS(context.Background(), casReq)
	for {
		if res.GetRedirect() != nil {
			//if reflect.TypeOf(res.Result).String() == "*pb.Result_Redirect" {
			reconnect(res, &kvc)
			log.Printf("reconnected")
			res, err = kvc.CAS(context.Background(), casReq)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hello" || res.GetKv().Value == "3" {
		log.Fatalf("Get returned the wrong response")
	}

	time.Sleep(2 * time.Second)
	// CAS should fail for uninitialized variables
	casReq = &pb.CASArg{Kv: &pb.KeyValue{Key: "hellooo", Value: "1"}, Value: &pb.Value{Value: "2"}}
	res, err = kvc.CAS(context.Background(), casReq)
	for {
		if res.GetRedirect() != nil {
			//if reflect.TypeOf(res.Result).String() == "*pb.Result_Redirect" {
			reconnect(res, &kvc)
			log.Printf("reconnected")
			res, err = kvc.CAS(context.Background(), casReq)
		} else {
			break
		}
	}
	if err != nil {
		log.Fatalf("Request error %v", err)
	}
	log.Printf("Got response key: \"%v\" value:\"%v\"", res.GetKv().Key, res.GetKv().Value)
	if res.GetKv().Key != "hellooo" || res.GetKv().Value == "2" {
		log.Fatalf("Get returned the wrong response")
	}
}
