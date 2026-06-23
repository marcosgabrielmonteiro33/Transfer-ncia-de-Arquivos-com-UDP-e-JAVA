```java
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ServidorUDP {

    // Porta principal onde o servidor UDP ficará aguardando requisições dos clientes
    private static final int PORTA_SERVIDOR = 9876;

    // Tamanho do buffer usado para receber mensagens e pacotes UDP
    private static final int TAMANHO_BUFFER = 2048;

    // Tamanho máximo dos dados de arquivo enviados em cada pacote
    private static final int TAMANHO_DADOS = 1000;

    // Porcentagem usada para simular perda de pacotes durante os testes
    private static final double PERDA_PACOTES = 0.15; // 15% de perda simulada

    // Pasta onde os arquivos recebidos pelo servidor serão salvos
    private static final File PASTA_SERVIDOR = new File("arquivos_recebidos");

    public static void main(String[] args) throws Exception {

        // Cria a pasta arquivos_recebidos caso ela ainda não exista
        if (!PASTA_SERVIDOR.exists()) {
            PASTA_SERVIDOR.mkdirs();
        }

        // Cria o socket UDP principal do servidor na porta 9876
        DatagramSocket socketPrincipal = new DatagramSocket(PORTA_SERVIDOR);

        System.out.println("Servidor UDP iniciado na porta " + PORTA_SERVIDOR);
        System.out.println("Pasta dos arquivos: " + PASTA_SERVIDOR.getAbsolutePath());

        // Mantém o servidor sempre em execução aguardando requisições dos clientes
        while (true) {

            // Cria o pacote que será usado para receber mensagens UDP
            byte[] buffer = new byte[TAMANHO_BUFFER];
            DatagramPacket pacote = new DatagramPacket(buffer, buffer.length);

            // Aguarda uma requisição UDP do cliente
            socketPrincipal.receive(pacote);

            // Converte os bytes recebidos em texto
            String mensagem = new String(
                    pacote.getData(),
                    0,
                    pacote.getLength(),
                    StandardCharsets.UTF_8
            );

            // Guarda o IP e a porta do cliente que enviou a requisição
            InetAddress enderecoCliente = pacote.getAddress();
            int portaCliente = pacote.getPort();

            System.out.println("\nRequisição recebida: " + mensagem);

            // Verifica se o cliente deseja enviar um arquivo para o servidor
            if (mensagem.startsWith("UPLOAD|")) {

                // Divide a mensagem para pegar o nome do arquivo e o total de pacotes
                String[] partes = mensagem.split("\\|", 3);
                String nomeArquivo = partes[1];
                int totalPacotes = Integer.parseInt(partes[2]);

                // Cria uma porta temporária para receber os pacotes do arquivo
                DatagramSocket socketTransferencia = new DatagramSocket(0);
                int portaTransferencia = socketTransferencia.getLocalPort();

                // Envia ao cliente a porta temporária que será usada para o upload
                enviarTexto(
                        socketPrincipal,
                        "UPLOAD_PORT|" + portaTransferencia,
                        enderecoCliente,
                        portaCliente
                );

                System.out.println("Upload será recebido na porta " + portaTransferencia);

                // Cria uma nova thread para receber o arquivo sem travar o servidor principal
                new Thread(() -> receberArquivo(socketTransferencia, nomeArquivo, totalPacotes)).start();

                // Verifica se o cliente deseja listar os arquivos disponíveis no servidor
            } else if (mensagem.equals("LIST")) {

                // Busca os arquivos salvos na pasta do servidor
                String lista = listarArquivos();

                // Envia a lista de arquivos para o cliente
                enviarTexto(
                        socketPrincipal,
                        "FILES|" + lista,
                        enderecoCliente,
                        portaCliente
                );

                // Verifica se o cliente deseja baixar um arquivo do servidor
            } else if (mensagem.startsWith("DOWNLOAD|")) {

                // Pega o nome do arquivo solicitado pelo cliente
                String[] partes = mensagem.split("\\|", 2);
                String nomeArquivo = partes[1];

                // Procura o arquivo dentro da pasta arquivos_recebidos
                File arquivo = new File(PASTA_SERVIDOR, nomeArquivo);

                // Se o arquivo não existir, envia mensagem de erro ao cliente
                if (!arquivo.exists()) {
                    enviarTexto(
                            socketPrincipal,
                            "ERRO|Arquivo não encontrado",
                            enderecoCliente,
                            portaCliente
                    );
                    continue;
                }

                // Calcula o total de pacotes necessários para enviar o arquivo
                int totalPacotes = calcularTotalPacotes(arquivo.length());

                // Cria uma porta temporária para enviar os pacotes do arquivo
                DatagramSocket socketTransferencia = new DatagramSocket(0);
                int portaTransferencia = socketTransferencia.getLocalPort();

                // Informa ao cliente a porta temporária e o total de pacotes do download
                enviarTexto(
                        socketPrincipal,
                        "DOWNLOAD_PORT|" + portaTransferencia + "|" + totalPacotes,
                        enderecoCliente,
                        portaCliente
                );

                System.out.println("Download será enviado pela porta " + portaTransferencia);

                // Cria uma nova thread para enviar o arquivo sem travar o servidor principal
                new Thread(() -> enviarArquivo(socketTransferencia, arquivo)).start();

            } else {

                // Caso o comando recebido não seja reconhecido
                enviarTexto(
                        socketPrincipal,
                        "ERRO|Comando inválido",
                        enderecoCliente,
                        portaCliente
                );
            }
        }
    }

    private static void receberArquivo(DatagramSocket socket, String nomeArquivo, int totalPacotes) {
        try {

            // Map usado para guardar os pacotes recebidos pelo número de sequência
            Map<Integer, byte[]> pacotesRecebidos = new HashMap<>();

            // Objeto usado para simular perda aleatória de pacotes
            Random random = new Random();

            System.out.println("Recebendo arquivo: " + nomeArquivo);

            // Continua recebendo até que todos os pacotes tenham chegado
            while (pacotesRecebidos.size() < totalPacotes) {

                byte[] buffer = new byte[TAMANHO_BUFFER];
                DatagramPacket datagrama = new DatagramPacket(buffer, buffer.length);

                // Recebe um pacote UDP contendo parte do arquivo
                socket.receive(datagrama);

                // Lê o pacote recebido e extrai tipo, sequência, total e dados
                PacoteArquivo pacote = lerPacote(datagrama.getData(), datagrama.getLength());

                // Simula perda de pacotes: ignora o pacote e não envia ACK
                if (random.nextDouble() < PERDA_PACOTES) {
                    System.out.println("[SIMULAÇÃO] Pacote " + pacote.sequencia + " ignorado. ACK não enviado.");
                    continue;
                }

                // Armazena o pacote recebido usando o número de sequência como chave
                pacotesRecebidos.put(pacote.sequencia, pacote.dados);

                System.out.println("Pacote " + pacote.sequencia + " recebido. Enviando ACK " + pacote.sequencia);

                // Envia ACK confirmando o recebimento do pacote
                enviarTexto(
                        socket,
                        "ACK|" + pacote.sequencia,
                        datagrama.getAddress(),
                        datagrama.getPort()
                );
            }

            // Cria o arquivo final dentro da pasta arquivos_recebidos
            File arquivoFinal = new File(PASTA_SERVIDOR, nomeArquivo);

            // Reconstrói o arquivo escrevendo os pacotes na ordem correta
            try (FileOutputStream fos = new FileOutputStream(arquivoFinal)) {
                for (int i = 0; i < totalPacotes; i++) {
                    fos.write(pacotesRecebidos.get(i));
                }
            }

            System.out.println("Arquivo recebido e reconstruído com sucesso:");
            System.out.println(arquivoFinal.getAbsolutePath());

            // Fecha o socket usado nessa transferência
            socket.close();

        } catch (Exception e) {
            System.out.println("Erro ao receber arquivo: " + e.getMessage());
            socket.close();
        }
    }

    private static void enviarArquivo(DatagramSocket socket, File arquivo) {
        try {

            // Lê todo o conteúdo do arquivo que será enviado ao cliente
            byte[] conteudo = lerArquivoCompleto(arquivo);

            // Calcula o total de pacotes necessários para enviar o arquivo inteiro
            int totalPacotes = calcularTotalPacotes(conteudo.length);

            // Aguarda uma mensagem READY do cliente antes de iniciar o envio
            byte[] bufferReady = new byte[TAMANHO_BUFFER];
            DatagramPacket ready = new DatagramPacket(bufferReady, bufferReady.length);

            socket.receive(ready);

            InetAddress enderecoCliente = ready.getAddress();
            int portaCliente = ready.getPort();

            // Define o tempo máximo de espera por um ACK
            socket.setSoTimeout(1000);

            System.out.println("Enviando arquivo: " + arquivo.getName());

            // Envia cada pacote do arquivo usando Stop-and-Wait
            for (int sequencia = 0; sequencia < totalPacotes; sequencia++) {

                // Calcula o início e o tamanho do pedaço do arquivo que será enviado
                int inicio = sequencia * TAMANHO_DADOS;
                int tamanho = Math.min(TAMANHO_DADOS, conteudo.length - inicio);

                // Copia apenas a parte do arquivo correspondente ao pacote atual
                byte[] dadosPacote = Arrays.copyOfRange(conteudo, inicio, inicio + tamanho);

                // Cria o pacote com número de sequência, total de pacotes e dados
                byte[] pacoteBytes = criarPacote(sequencia, totalPacotes, dadosPacote);

                boolean ackRecebido = false;

                // Stop-and-Wait: envia um pacote e aguarda o ACK antes de continuar
                while (!ackRecebido) {

                    DatagramPacket pacote = new DatagramPacket(
                            pacoteBytes,
                            pacoteBytes.length,
                            enderecoCliente,
                            portaCliente
                    );

                    // Envia o pacote UDP para o cliente
                    socket.send(pacote);

                    System.out.println("Pacote " + sequencia + " enviado. Aguardando ACK...");

                    try {
                        byte[] bufferAck = new byte[TAMANHO_BUFFER];
                        DatagramPacket pacoteAck = new DatagramPacket(bufferAck, bufferAck.length);

                        // Aguarda o ACK do cliente
                        socket.receive(pacoteAck);

                        String ack = new String(
                                pacoteAck.getData(),
                                0,
                                pacoteAck.getLength(),
                                StandardCharsets.UTF_8
                        );

                        // Se o ACK recebido for do pacote atual, passa para o próximo pacote
                        if (ack.equals("ACK|" + sequencia)) {
                            System.out.println("ACK " + sequencia + " recebido.");
                            ackRecebido = true;
                        }

                    } catch (SocketTimeoutException e) {

                        // Se o ACK não chegar no tempo limite, o pacote é retransmitido
                        System.out.println("TIMEOUT no pacote " + sequencia + ". Retransmitindo...");
                    }
                }
            }

            System.out.println("Arquivo enviado com sucesso.");
            socket.close();

        } catch (Exception e) {
            System.out.println("Erro ao enviar arquivo: " + e.getMessage());
            socket.close();
        }
    }

    private static String listarArquivos() {

        // Lista os arquivos da pasta arquivos_recebidos
        String[] arquivos = PASTA_SERVIDOR.list();

        // Caso não exista nenhum arquivo na pasta
        if (arquivos == null || arquivos.length == 0) {
            return "Nenhum arquivo disponível";
        }

        // Retorna os nomes dos arquivos separados por vírgula
        return String.join(",", arquivos);
    }

    private static int calcularTotalPacotes(long tamanhoArquivo) {

        // Calcula quantos pacotes serão necessários para enviar o arquivo
        return (int) Math.ceil(tamanhoArquivo / (double) TAMANHO_DADOS);
    }

    private static byte[] lerArquivoCompleto(File arquivo) throws IOException {

        // Lê todos os bytes de um arquivo
        try (FileInputStream fis = new FileInputStream(arquivo)) {
            return fis.readAllBytes();
        }
    }

    private static void enviarTexto(DatagramSocket socket, String texto, InetAddress endereco, int porta) {
        try {

            // Converte a mensagem de texto para bytes
            byte[] dados = texto.getBytes(StandardCharsets.UTF_8);

            // Cria o pacote UDP com a mensagem
            DatagramPacket pacote = new DatagramPacket(
                    dados,
                    dados.length,
                    endereco,
                    porta
            );

            // Envia a mensagem por UDP
            socket.send(pacote);

        } catch (IOException e) {
            System.out.println("Erro ao enviar texto: " + e.getMessage());
        }
    }

    private static byte[] criarPacote(int sequencia, int totalPacotes, byte[] dados) throws IOException {

        // Monta um pacote com cabeçalho e dados do arquivo
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Tipo do pacote
        dos.writeUTF("DATA");

        // Número de sequência do pacote
        dos.writeInt(sequencia);

        // Total de pacotes do arquivo
        dos.writeInt(totalPacotes);

        // Tamanho dos dados enviados neste pacote
        dos.writeInt(dados.length);

        // Dados do arquivo
        dos.write(dados);
        dos.flush();

        // Retorna o pacote completo em formato de bytes
        return baos.toByteArray();
    }

    private static PacoteArquivo lerPacote(byte[] bytes, int tamanho) throws IOException {

        // Lê os bytes recebidos e separa as informações do pacote
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes, 0, tamanho);
        DataInputStream dis = new DataInputStream(bais);

        String tipo = dis.readUTF();
        int sequencia = dis.readInt();
        int totalPacotes = dis.readInt();
        int tamanhoDados = dis.readInt();

        byte[] dados = new byte[tamanhoDados];
        dis.readFully(dados);

        // Retorna um objeto representando o pacote recebido
        return new PacoteArquivo(tipo, sequencia, totalPacotes, dados);
    }

    static class PacoteArquivo {

        // Tipo do pacote, por exemplo DATA
        String tipo;

        // Número de sequência do pacote
        int sequencia;

        // Total de pacotes esperados
        int totalPacotes;

        // Dados reais do arquivo
        byte[] dados;

        PacoteArquivo(String tipo, int sequencia, int totalPacotes, byte[] dados) {
            this.tipo = tipo;
            this.sequencia = sequencia;
            this.totalPacotes = totalPacotes;
            this.dados = dados;
        }
    }
}
```
