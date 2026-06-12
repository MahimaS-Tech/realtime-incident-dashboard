FROM golang:1.24-alpine AS build
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/incident-server ./cmd/server

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
USER 65532:65532
COPY --from=build /out/incident-server /incident-server
EXPOSE 8080
ENTRYPOINT ["/incident-server"]
