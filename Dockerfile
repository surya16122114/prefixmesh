FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /bin/
ENTRYPOINT ["/bin/gateway"]
