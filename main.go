package main

import "fmt"

func Run() error {
	fmt.Println("Go RabbitMQ")
	return nil
}

func main() {
	if err := Run(); err != nil {
		fmt.Println("Error setting up the application")
		fmt.Println(err)
	}
}