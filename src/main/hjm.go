package main

import "fmt"

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
}
