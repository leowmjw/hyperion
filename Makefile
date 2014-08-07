development:
	go build -ldflags "-X main.buildVersion -dev.`date -u +%Y%m%d`"
