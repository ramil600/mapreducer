This is the coordinated map and reduce jobs implementer.
Usage:
> cd src/main

> go run mrcoordinator.go pg*.txt

> go build -buildmode=plugin ../mrappas/wc.go

> go run mrworker.go wc.so&
