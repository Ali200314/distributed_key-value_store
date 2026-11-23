package main

import (
	"clientdriver"
	"context"
	"fmt"
	"net/http"
)

func main() {
	httpClient := &http.Client{}
	driver, err := clientdriver.New("http://127.0.0.1:8073", httpClient)
	if err != nil {
		panic(err)
	}

	for {
		fmt.Println("1 - Create")
		fmt.Println("2 - Read")
		fmt.Println("3 - Update")
		fmt.Println("4 - Delete")

		var num int
		fmt.Scan(&num)

		switch num {
		case 1:
			create(driver)
		case 2:
			read(driver)
		case 3:
			update(driver)
		case 4:
			delete(driver)
		}

	}

}

func create(driver clientdriver.Driver) {
	fmt.Println("Enter key")
	var key string
	fmt.Scan(&key)
	fmt.Println("Enter value")
	var value string
	fmt.Scan(&value)
	err := driver.Create(context.Background(), key, value)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Success")
}
func read(driver clientdriver.Driver) {
	fmt.Println("Enter key")
	var key string
	fmt.Scan(&key)
	for {
		val, err := driver.Read(context.Background(), key)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Printf("%s\n", val)
	}
}

func update(driver clientdriver.Driver) {
	fmt.Println("Enter key")
	var key string
	fmt.Scan(&key)
	fmt.Println("Enter value")
	var value string
	fmt.Scan(&value)
	err := driver.Update(context.Background(), key, value)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Success")
}
func delete(driver clientdriver.Driver) {

	fmt.Println("Enter key")
	var key string
	fmt.Scan(&key)
	err := driver.Delete(context.Background(), key)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Success")
}
