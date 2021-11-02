# Changelog

## v0.6.3
> 02 Nov 2021

- Add all amqp.Publishing options to internal PublishOptions
- Add notify on return channel for publisher
- Add queue args to ConsumeOptions
- Switch from streadway/amqp to official rabbitmq/amqp091-go
- Allow consumer handler functions to Ack, NackDiscard, NackRequeue
- Add option to skip exchange declaration for consumer