package main

import (
    "encoding/json"
    "fmt"
    "log"
    "net/http"
    "os"
    "os/signal"
    "strconv"
    "strings"
    "time"

    "github.com/garyburd/redigo/redis"
    "github.com/labstack/echo"
    "github.com/labstack/echo/middleware"
    "github.com/streadway/amqp"
)

func main() {

    conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
    failOnError(err, "Failed to connect to RabbitMQ")
    defer conn.Close()

    ch, err := conn.Channel()
    failOnError(err, "Failed to open a channel")
    defer ch.Close()

    q, err := ch.QueueDeclare(
        "logs",
        false,
        false,
        false,
        false,
        nil,
    )
    failOnError(err, "Failed to declare a queue")

    msgs, err := ch.Consume(
        q.Name, // queue
        "",
        true,
        false,
        false,
        false,
        nil,
    )
    failOnError(err, "Failed to register a consumer")

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)

    msgCount := 0

    doneCh := make(chan struct{})
    go func() {
        for {
            select {
            case d := <- msgs:
                if string(d.Body) == "" {
                    //fmt.Println(d.Body)
                } else {
                    message := new(Message)
                    err := json.Unmarshal(d.Body, message)
                    if err != nil {
                        log.Printf("Error marshalling json", err)
                    }

                    if message.Type == "logs" {
                        if strings.Contains(
                            message.Payload.(string), "Mh/s") == true {
                            if strings.Contains(
                                message.Payload.(string),
                                "Total Speed") == true {

                                messageArray := strings.Split(
                                    message.Payload.(string), " ")
                                for i := range messageArray {
                                    if messageArray[i] == "Speed:" {
                                        UpdateTotalHashRate(messageArray[i+1])
                                    }
                                }
                            } else if strings.Contains(
                                message.Payload.(string), "GPU") {
                                messageArray := strings.Split(
                                    message.Payload.(string), " ")

                                numGpus := strings.Count(
                                    message.Payload.(string), "GPU")

                                var gpus []*Gpu
                                for i := 0; i < numGpus; i++ {
                                    gpu := new(Gpu)
                                    gpu.Id = i
                                    gpu.HashRate = messageArray[2 + (i * 3)]
                                    gpus = append(gpus, gpu)
                                }

                                UpdateGpus(gpus)
                            }
                        } else if strings.Contains(
                            message.Payload.(string), "t=") == true {
                            //fmt.Println(message.Payload.(string))
                        }
                        //go UpdatePing(message.Payload.(map[string]interface{}))
                    }

                    msgCount++
                }
            case <-signals:
                fmt.Println("Interrupt is detected")
                doneCh <- struct{}{}
            }
        }
    }()

    go StartHttpServer()
    fmt.Println("Up and running")

    <-doneCh
    fmt.Println("Processed", msgCount, "messages")

}

type Message struct {
    SenderId 		string					`json:"mac"`
    UserHash 		string					`json:"mac"`
    Type 			string					`json:"type"`
    Payload 		interface{}				`json:"payload"`
}

func StartHttpServer() {

    e := echo.New()
    e.Use(middleware.CORS())

    e.Use(middleware.Logger())
    e.Use(middleware.Recover())

    e.GET("/", Handler)
    e.GET("/create", CreateRig)
    e.Logger.Fatal(e.Start(":5052"))
}

func Handler(c echo.Context) error {

    m := Find("1")
    return c.JSON(http.StatusOK, m)
}

func CreateRig(c echo.Context) error {

    rig := new(Rig)
    Create(*rig)

    return nil
}

type Rig struct {
    Id 				int 			`json:"id"`
    AccountId 		string 			`json:"account_id"`
    Timestamp 		time.Time		`json:"timestamp"`
    LastPing		Ping 			`json:"last_ping"`
    TotalHashRate   string          `json:"total_hash_rate"`
    Gpus            []*Gpu          `json:"gpus"`
}

type Gpu struct {
    Id              int             `json:"id"`
    HashRate        string          `json:"hash_rate"`
}

func Create(r Rig) {

    r.Id = 1
    r.Timestamp = time.Now()
    c := RedisConnect()
    defer c.Close()

    b, err := json.Marshal(r)
    HandleError(err)

    reply, err := c.Do(
        "SET", "machine:" + strconv.Itoa(r.Id), b)
    HandleError(err)

    fmt.Println("GET ", reply)
}

func Find(id string) Rig {

    var r Rig

    c := RedisConnect()
    defer c.Close()

    reply, err := c.Do("GET", "machine:" + id)
    HandleError(err)

    if err = json.Unmarshal(reply.([]byte), &r); err != nil {
        panic(err)
    }
    return r
}

func HandleError(err error) {

    if err != nil {
        panic(err)
    }
}

func RedisConnect() redis.Conn {

    c, err := redis.Dial("tcp", "localhost:6379")
    HandleError(err)
    return c
}

func UpdateRig(r Rig) {

    r.Timestamp = time.Now()
    c := RedisConnect()
    defer c.Close()

    b, err := json.Marshal(r)
    HandleError(err)

    reply, err := c.Do(
        "SET", "machine:" + strconv.Itoa(r.Id), b)
    HandleError(err)

    fmt.Println("GET ", reply)
}

func UpdateTotalHashRate(hashRate string) {

    rig := Find("1")
    rig.TotalHashRate = hashRate

    UpdateRig(rig)
}

func UpdateGpus(gpus []*Gpu) {
    rig := Find("1")
    rig.Gpus = gpus

    UpdateRig(rig)
}

type Ping struct {
    Time 		time.Time		`json:"time"`
}

//func FindAll() [][]byte {
//
//	var posts [][]byte
//
//	c := RedisConnect()
//	defer c.Close()
//
//	keys, err := c.Do("KEYS", "post:*")
//	HandleError(err)
//
//	for _, k := range keys.([]interface{}) {
//
//		var post []byte
//
//		reply, err := c.Do("GET", k.([]byte))
//		HandleError(err)
//
//		if err := json.Unmarshal(reply.([]byte), &post); err != nil {
//			panic(err)
//		}
//		posts = append(posts, post)
//	}
//	return posts
//}

func failOnError(err error, msg string) {

    if err != nil {
        log.Fatalf("%s: %s", msg, err)
    }
}
