package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/rds"
	"github.com/urfave/cli"
	"github.com/gdsace/rdstail/src"
)

func fie(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}

func signalListen(stop chan<- struct{}) {
	c := make(chan os.Signal)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGINT)

	<-c
	close(stop)
	<-c
	log.Panic("Aborting on second signal")
}

func setupRDS(c *cli.Context) *rds.RDS {
	region := c.String("region")
	maxRetries := c.Int("max-retries")
	cfg := aws.NewConfig().WithRegion(region).WithMaxRetries(maxRetries)
	return rds.New(session.New(), cfg)
}

func parseRate(c *cli.Context) time.Duration {
	rate, err := time.ParseDuration(c.String("rate"))
	fie(err)
	return rate
}

func parseDB(c *cli.Context) string {
	db := c.String("instance")
	if db == "" {
		fie(errors.New("-instance required"))
	}
	return db
}

func watch(c *cli.Context) error {
	r := setupRDS(c)
	db := parseDB(c)
	rate := parseRate(c)
	prefixPattern := c.String("prefix")
	fmt.Println(prefixPattern)

	stop := make(chan struct{})
	go signalListen(stop)

	err := rdstail.Watch(r, db, rate, prefixPattern, func(lines string) error {
		fmt.Print(lines)
		return nil
	}, stop)

	defer fie(err)
	return nil
}

func papertrail(c *cli.Context) error {
	r := setupRDS(c)
	db := parseDB(c)
	rate := parseRate(c)
	papertrailHost := c.String("papertrail")
	if papertrailHost == "" {
		defer fie(errors.New("-papertrail required"))
		return nil
	}
	appName := c.String("app")
	hostname := c.String("hostname")
	if hostname == "os.Hostname()" {
		var err error
		hostname, err = os.Hostname()
		defer fie(err)
		return nil
	}

	stop := make(chan struct{})
	go signalListen(stop)

	err := rdstail.FeedPapertrail(r, db, rate, papertrailHost, appName, hostname, stop)

	defer fie(err)
	return nil
}

func tail(c *cli.Context) error {
	r := setupRDS(c)
	db := parseDB(c)
	numLines := int64(c.Int("lines"))
	prefixPattern := c.String("prefix")
	err := rdstail.Tail(r, db, numLines, prefixPattern)
	defer fie(err)
	return nil
}

func main() {
	app := cli.NewApp()

	app.Name = "rdstail"
	app.Usage = `Reads AWS RDS logs

    AWS credentials are taken from an ~/.aws/credentials file or the env vars AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY.`
	app.Version = "0.1.0"
	app.Flags = []cli.Flag{
		&cli.StringFlag{
			Name:    "instance",
			Aliases: []string{"i"},
			Usage:   "Name of the db instance in RDS [required]",
		},
		&cli.StringFlag{
			Name:    "prefix",
			Aliases: []string{"p"}
			Value:   "",
			Prefix:  "Prefix regexp pattern to detect when a new log line begins"
		},
		&cli.StringFlag{
			Name:    "region",
			Value:   "us-east-1",
			Usage:   "AWS region",
			EnvVars: []string{"AWS_REGION"},
		},
		&cli.IntFlag{
			Name:  "max-retries",
			Value: 10,
			Usage: "Maximium number of retries for RDS requests",
		},
	}

	app.Commands = []*cli.Command{
		{
			Name:   "papertrail",
			Usage:  "stream logs into papertrail",
			Action: papertrail,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "papertrail",
					Aliases: []string{"p"},
					Value: "",
					Usage: "Papertrail host e.g. logs.papertrailapp.com:8888 [required]",
				},
				&cli.StringFlag{
					Name:  "app",
					Aliases: []string{"a"},
					Value: "rdstail",
					Usage: "App name to send to papertrail",
				},
				&cli.StringFlag{
					Name:  "hostname",
					Value: "os.Hostname()",
					Usage: "Gostname of the client, sent to papertrail",
				},
				&cli.StringFlag{
					Name:  "rate",
					Aliases: []string{"r"},
					Value: "3s",
					Usage: "RDS log polling rate",
				},
			},
		},

		{
			Name:   "watch",
			Usage:  "stream logs to stdout",
			Action: watch,
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  "rate",
					Aliases: []string{"r"},
					Value: "3s",
					Usage: "RDS log polling rate",
				},
			},
		},

		{
			Name:   "tail",
			Usage:  "tail the last N lines",
			Action: tail,
			Flags: []cli.Flag{
				&cli.IntFlag{
					Name:  "lines",
					Aliases: []string{"n"},
					Value: 20,
					Usage: "Output the last n lines. use 0 for a full dump of the most recent file",
				},
			},
		},
	}

	app.Run(os.Args)
}
