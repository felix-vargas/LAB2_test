package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"time"

	pb "github.com/Kendovvul/Ejemplo/Proto"
	"google.golang.org/grpc"
)

var file, err = os.Create("DATA.txt")

func Ordenar(texto string, tipo string) string {
	file1, err1 := os.Open("DATA.txt")

	if err1 != nil {
		log.Fatalf("failed creating file: %s", err1)
	}

	Split_Msj := strings.Split(texto, "\n")

	var lista []string

	scanner := bufio.NewScanner(file1)

	for scanner.Scan() {

		Split_Msj1 := strings.Split(scanner.Text(), ":")

		if Split_Msj1[0] == tipo {
			lista = append(lista, Split_Msj1[1])
		}
	}

	resultado := ""

	n := len(lista)
	m := len(Split_Msj)

	for i := 0; i < n; i++ {

		for p := 0; p < m; p++ {

			sp := strings.Split(Split_Msj[p], ":")
			if lista[i] == sp[0] {
				resultado = resultado + Split_Msj[p] + "\n"
			}

		}
	}

	file1.Close()

	return resultado
}

func RevisarID(ID string) bool {

	file1, err1 := os.Open("DATA.txt")

	if err1 != nil {
		log.Fatalf("failed creating file: %s", err1)
	}

	scanner := bufio.NewScanner(file1)

	for scanner.Scan() {

		Split_Msj := strings.Split(scanner.Text(), ":")
		if Split_Msj[1] == ID {

			return false

		}
	}

	file1.Close()
	return true

}

func DateNodeRandom() (Nombre_DateNode string, IP string) {
	rand.Seed(time.Now().UnixNano())
	switch os := rand.Intn(3); os {
	case 0:
		Nombre := "DateNode Grunt"
		IP := "dist042:50051"
		return Nombre, IP
	case 1:
		Nombre := "DateNode Synth"
		IP := "dist043:50051"
		return Nombre, IP
	default:
		Nombre := "DateNode Cremator"
		IP := "dist044:50051"
		return Nombre, IP
	}
}

func GuardarDATA(data string) {

	Split_Msj := strings.Split(data, ":")
	Tipo := Split_Msj[0]
	ID := Split_Msj[1]

	NAMEDATENODE, IPNODE := DateNodeRandom()

	_, err := file.WriteString(Tipo + ":" + ID + ":" + NAMEDATENODE + "\n")

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	err = file.Sync()

	if err != nil {
		log.Fatalf("failed creating file: %s", err)
	}

	connS, err := grpc.Dial(IPNODE, grpc.WithInsecure())

	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}

	defer connS.Close()

	serviceCliente := pb.NewMessageServiceClient(connS)

	//envia el mensaje al laboratorio
	res, err := serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "0:" + data,
		})

	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	fmt.Println(res.Body)       //respuesta del laboratorio
	time.Sleep(1 * time.Second) //espera de 5 segundos

}

func Fetch_Rebeldes(tipo string) string {

	Respuesta := ""

	//CONEXION DATANODE 1
	connS, err := grpc.Dial("dist042:50051", grpc.WithInsecure())
	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}
	defer connS.Close()

	serviceCliente := pb.NewMessageServiceClient(connS)

	res, err := serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "1:" + tipo,
		})
	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	Respuesta = Respuesta + res.Body

	//CONEXION DATANODE 2
	connS, err = grpc.Dial("dist043:50051", grpc.WithInsecure())

	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}

	defer connS.Close()

	serviceCliente = pb.NewMessageServiceClient(connS)

	res, err = serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "1:" + tipo,
		})
	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	Respuesta = Respuesta + res.Body

	//CONEXION DATANODE 3
	connS, err = grpc.Dial("dist044:50051", grpc.WithInsecure())
	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}

	defer connS.Close()

	serviceCliente = pb.NewMessageServiceClient(connS)

	res, err = serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "1:" + tipo,
		})
	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	Respuesta = Respuesta + res.Body

	RetornarString := Ordenar(Respuesta, tipo)

	return RetornarString
}

func Cierre() string {

	//CONEXION DATANODE 1
	connS, err := grpc.Dial("dist042:50051", grpc.WithInsecure())
	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}
	defer connS.Close()

	serviceCliente := pb.NewMessageServiceClient(connS)

	_, err = serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "CIERRE",
		})
	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	//CONEXION DATANODE 2
	connS, err = grpc.Dial("dist043:50051", grpc.WithInsecure())

	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}

	defer connS.Close()

	serviceCliente = pb.NewMessageServiceClient(connS)

	_, err = serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "CIERRE",
		})
	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	//CONEXION DATANODE 3
	connS, err = grpc.Dial("dist044:50051", grpc.WithInsecure())
	if err != nil {
		panic("No se pudo conectar con el servidor" + err.Error())
	}

	defer connS.Close()

	serviceCliente = pb.NewMessageServiceClient(connS)

	_, err = serviceCliente.Intercambio(context.Background(),
		&pb.Message{
			Body: "CIERRE",
		})
	if err != nil {
		panic("No se puede crear el mensaje " + err.Error())
	}

	return "DATANODE 1,2 y 3 CERRADOS : NAMENODE CERRADO >>"

}

type server struct {
	pb.UnimplementedMessageServiceServer
}

func (s *server) Intercambio(ctx context.Context, msg *pb.Message) (*pb.Message, error) {

	msn := ""
	Split_Msj := strings.Split(msg.Body, ":")

	if Split_Msj[0] == "0" { //conbine
		ID := Split_Msj[2]
		Info := Split_Msj[1] + ":" + Split_Msj[2] + ":" + Split_Msj[3]
		if RevisarID(ID) == true {
			GuardarDATA(Info)
			msn = "Mensaje enviado exitosamente"

		} else {
			msn = "ID Repetido"
		}

		println("Solicitud desde Combine recibida, mensaje enviado: " + msn)

	}
	if Split_Msj[0] == "1" { //rebelde
		msn = Fetch_Rebeldes(Split_Msj[1])

		println("Solicitud desde Rebelde pidiendo datos de " + Split_Msj[1] + ", mensaje enviado: " + msn)

	}
	if msg.Body == "CIERRE" {
		Cierre()
		os.Exit(1)
	}

	return &pb.Message{Body: msn}, nil
}

func main() {

	defer file.Close()

	listener, err := net.Listen("tcp", ":50051") //conexion sincrona
	if err != nil {
		panic("La conexion no se pudo crear" + err.Error())
	}

	serv := grpc.NewServer()
	for {
		pb.RegisterMessageServiceServer(serv, &server{})
		if err = serv.Serve(listener); err != nil {
			panic("El server no se pudo iniciar" + err.Error())
		}
	}

}
