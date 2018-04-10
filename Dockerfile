FROM golang:latest

ENV RABBIT_ADDR='amqp://ltvdoacc:urQws_KDYLQbcK0mOidy48snnJQMsr7Z@wombat.rmq.cloudamqp.com/ltvdoacc'
ENV REDIS_ADDR='redis-master:6379'

ADD . /go/src/app
WORKDIR /go/src
RUN go get app
RUN go install app
ENTRYPOINT /go/bin/app -addr=$RABBIT_ADDR -redis=$REDIS_ADDR

