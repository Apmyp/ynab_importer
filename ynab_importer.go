package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := receiversFromDisk(ctx)
	fr := filteredReceivers(ctx, r)
	mf := messagesFiles(ctx, fr)
	out := parsedMessages(ctx, mf)

	for v := range out {
		fmt.Println(v)
	}
}

const receiversPath = "./messages"

type Receiver struct {
	name string
}

func receiversFromDisk(ctx context.Context) <-chan Receiver {
	entries, err := os.ReadDir(receiversPath)
	if err != nil {
		log.Fatal(err)
	}

	out := make(chan Receiver)

	go func() {
		defer close(out)
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			receiver := Receiver{name: e.Name()}
			if !allowedReceiver(receiver) {
				continue
			}
			select {
			case out <- receiver:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func filteredReceivers(ctx context.Context, in <-chan Receiver) <-chan Receiver {
	out := make(chan Receiver)

	go func() {
		defer close(out)
		for receiver := range in {
			select {
			case out <- receiver:
			case <-ctx.Done():
				return
			}
		}
	}()

	return out
}

func allowedReceiver(receiver Receiver) bool {
	return receiver.name == "102" || receiver.name == "EXIMBANK"
}

type MessagesFile struct {
	receiver Receiver
	filepath string
}

func messagesFiles(ctx context.Context, in <-chan Receiver) <-chan MessagesFile {
	out := make(chan MessagesFile)

	go func() {
		defer close(out)
		for receiver := range in {
			folder := path.Join(receiversPath, receiver.name)
			entries, err := os.ReadDir(folder)
			if err != nil {
				log.Fatal(err)
			}

			for _, e := range entries {
				if e.IsDir() {
					continue
				}

				messagesFile := MessagesFile{receiver: receiver, filepath: path.Join(folder, e.Name())}
				select {
				case out <- messagesFile:
				case <-ctx.Done():
					return
				}
			}

		}
	}()

	return out
}

type Message struct {
	receiver  Receiver
	timestamp string
	body      string
}

func parsedMessages(ctx context.Context, in <-chan MessagesFile) <-chan Message {
	out := make(chan Message)

	go func() {
		defer close(out)
		for mf := range in {
			data, err := os.ReadFile(mf.filepath)
			if err != nil {
				log.Fatal(err)
			}

			pattern := regexp.MustCompile(`\[(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2})\]([^\[\]]+)`)
			matches := pattern.FindAllStringSubmatch(string(data), -1)

			for _, m := range matches {
				if len(m) != 3 {
					continue
				}
				timestamp := strings.TrimSpace(m[1])
				body := strings.Join(strings.Fields(strings.TrimSpace(m[2])), " ")
				message := Message{receiver: mf.receiver, timestamp: timestamp, body: body}
				select {
				case out <- message:
				case <-ctx.Done():
					return
				}
			}

		}
	}()

	return out
}
