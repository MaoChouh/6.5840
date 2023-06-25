package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

// 值传递
func valueFunc(x int) {
	x = 10
}

// 引用传递
func referenceFunc(x *int) {
	*x = 10
}

func main() {
	num := 5

	// 值传递，不会修改原始变量的值
	valueFunc(num)
	fmt.Println("值传递后的值:", num)

	// 引用传递，会修改原始变量的值
	referenceFunc(&num)
	fmt.Println("引用传递后的值:", num)

	sub_path := "to-delete/sdf/"
	err := os.Mkdir(sub_path, 0777)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	tmp_file, err := ioutil.TempFile(sub_path, "test*.json")
	if err != nil {
		log.Fatal(err)
	}
	my_map := map[int]string{}
	my_map[1] = "1"
	enc := json.NewEncoder(tmp_file)
	_ = enc.Encode(&my_map)
	fmt.Printf("tmp_file name : %v\n", tmp_file.Name())
	tmp_file.Close()
	return

}
