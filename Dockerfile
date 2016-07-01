FROM golang

RUN mkdir -p /go/src/app
WORKDIR /go/src/app

ENTRYPOINT ["go-wrapper", "run"]
ENV GO15VENDOREXPERIMENT=1

COPY . /go/src/app

RUN go-wrapper download
RUN go-wrapper install

EXPOSE 9092
