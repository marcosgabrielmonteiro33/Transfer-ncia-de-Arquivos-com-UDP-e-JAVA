# Transfer-ncia-de-Arquivos-com-UDP-e-JAVA
Transferência Confiável de Arquivos com UDP

Projeto desenvolvido em Java para transferência de arquivos utilizando sockets UDP.

Como o UDP não garante entrega, ordem ou retransmissão automática, o projeto implementa manualmente mecanismos de confiabilidade, como:

* Numeração dos pacotes
* ACK de confirmação
* Timeout
* Retransmissão
* Simulação de perda de pacotes
* Reconstrução correta do arquivo

## Funcionalidades

* Enviar arquivos do cliente para o servidor
* Listar arquivos disponíveis no servidor
* Baixar arquivos do servidor para o cliente
* Comunicação via UDP
* Uso de multithreading no servidor

## Tecnologias utilizadas

* Java
* UDP
* DatagramSocket
* DatagramPacket
* Wireshark

A pasta `arquivos_recebidos` armazena os arquivos enviados ao servidor.

A pasta `downloads` armazena os arquivos baixados pelo cliente.

## Como executar

Compile os arquivos:

bash
javac ServidorUDP.java ClienteUDP.java


Execute o servidor:

bash
java ServidorUDP


Em outro terminal, execute o cliente:

bash
java ClienteUDP


## Estratégia utilizada

A estratégia utilizada foi **Stop-and-Wait**.

O remetente envia um pacote e aguarda o ACK antes de enviar o próximo. Se o ACK não chegar dentro do tempo limite, ocorre timeout e o pacote é retransmitido.


## Autor

Marcos Gabriel Monteiro
