FROM golang:1.20-alpine AS build

WORKDIR /app

COPY go.mod ./
COPY go.sum ./

RUN go mod download

COPY . ./

RUN go build -o /charon ./cmd/charon/main.go

FROM gcr.io/distroless/static-debian11

WORKDIR /

COPY --from=build /charon /charon

ENTRYPOINT [ "/charon"]