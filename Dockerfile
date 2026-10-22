FROM golang:1.27-alpine AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bento-mysql ./cmd/bento-mysql

FROM alpine:3.21

RUN apk add --no-cache ca-certificates \
 && adduser -D -u 10001 bento \
 && mkdir -p /var/lib/bento \
 && chown bento:bento /var/lib/bento

COPY --from=build /out/bento-mysql /usr/local/bin/bento-mysql

USER bento
ENTRYPOINT ["/usr/local/bin/bento-mysql"]
CMD ["-c", "/etc/bento/router.yaml"]
