package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

var producer pulsar.Producer

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		log.Fatalf("Não foi possível iniciar o client pulsar: %v", err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: "adaptative",
	})

	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	mensagem := bufio.NewScanner(os.Stdin)

	for mensagem.Scan() {
		if mensagem.Text() == "" {
			break
		}
		producerSend(mensagem.Text(), producer)
	}

}

func producerSend(mensagem string, producer pulsar.Producer) {

	_, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte(mensagem),
	})

	if err != nil {
		fmt.Println("Falha ao publicar mensagem", err)
	}
	fmt.Println("Mensagem publicada!")
}
