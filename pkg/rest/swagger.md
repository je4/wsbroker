# generate swagger / openapi v3.1 files

```bash
go run github.com/swaggo/swag/v2/cmd/swag init --v3.1 --instanceName WSBroker --parseDependency --parseInternal -g pkg/rest/api.go -o pkg/rest/docs
```
