```java
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

public class ClienteUDP {

    // Endereço do servidor. Como o teste está local, usamos localhost se for ultilizar em maquinas diferentes substitua o "localhost" pelo ip da maquina do servidor
    private static final String HOST_SERVIDOR = "localhost";

    // Porta principal onde o servidor UDP está aguardando requisições
    private static final int PORTA_SERVIDOR = 9876;

    // Tamanho do buffer usado para receber mensagens e pacotes UDP
    private static final int TAMANHO_BUFFER = 2048;

    // Tamanho máximo dos dados enviados em cada pacote
    private static final int TAMANHO_DADOS = 1000;

    // Tempo máximo de espera pelo ACK antes de retransmitir o pacote
    private static final int TIMEOUT = 1000;

    // Porcentagem usada para simular perda de pacotes no download
    private static final double PERDA_PACOTES = 0.15; // 15% de perda simulada

    // Pasta onde os arquivos baixados do servidor serão salvos
    private static final File PASTA_DOWNLOADS = new File("downloads");

    public static void main(String[] args) throws Exception {

        // Cria a pasta downloads caso ela ainda não exista
        if (!PASTA_DOWNLOADS.exists()) {
            PASTA_DOWNLOADS.mkdirs();
        }

        Scanner scanner = new Scanner(System.in);

        // Cria o socket UDP do cliente
        DatagramSocket socket = new DatagramSocket();

        // Busca o endereço IP do servidor
        InetAddress enderecoServidor = InetAddress.getByName(HOST_SERVIDOR);

        // Menu principal do cliente
        while (true) {
            System.out.println("\n===== CLIENTE UDP =====");
            System.out.println("1 - Enviar arquivo para o servidor");
            System.out.println("2 - Listar arquivos do servidor");
            System.out.println("3 - Baixar arquivo do servidor");
            System.out.println("0 - Sair");
            System.out.print("Escolha uma opção: ");

            String opcao = scanner.nextLine();

            if (opcao.equals("1")) {

                // Opção para enviar arquivo para o servidor
                System.out.print("Digite o caminho do arquivo: ");

                // Lê o caminho e remove aspas caso o usuário use "Copiar como caminho"
                String caminho = scanner.nextLine().trim().replace("\"", "");

                enviarArquivo(socket, enderecoServidor, caminho);

            } else if (opcao.equals("2")) {

                // Opção para solicitar a lista de arquivos do servidor
                listarArquivos(socket, enderecoServidor);

            } else if (opcao.equals("3")) {

                // Opção para baixar um arquivo do servidor
                System.out.print("Digite o nome do arquivo para baixar: ");
                String nomeArquivo = scanner.nextLine();

                baixarArquivo(socket, enderecoServidor, nomeArquivo);

            } else if (opcao.equals("0")) {

                // Encerra o cliente
                System.out.println("Cliente encerrado.");
                socket.close();
                break;

            } else {
                System.out.println("Opção inválida.");
            }
        }

        scanner.close();
    }

    private static void enviarArquivo(DatagramSocket socket, InetAddress enderecoServidor, String caminhoArquivo) {
        try {

            // Cria um objeto File com o caminho informado pelo usuário
            File arquivo = new File(caminhoArquivo);

            // Verifica se o arquivo realmente existe
            if (!arquivo.exists()) {
                System.out.println("Arquivo não encontrado.");
                return;
            }

            // Lê todo o conteúdo do arquivo em bytes
            byte[] conteudo = lerArquivoCompleto(arquivo);

            // Calcula quantos pacotes serão necessários para enviar o arquivo
            int totalPacotes = calcularTotalPacotes(conteudo.length);

            // Envia ao servidor o comando de upload com o nome do arquivo e total de pacotes
            String comando = "UPLOAD|" + arquivo.getName() + "|" + totalPacotes;

            enviarTexto(socket, comando, enderecoServidor, PORTA_SERVIDOR);

            // Aguarda o servidor informar a porta temporária para transferência
            byte[] bufferResposta = new byte[TAMANHO_BUFFER];
            DatagramPacket resposta = new DatagramPacket(bufferResposta, bufferResposta.length);
            socket.receive(resposta);

            String mensagem = new String(
                    resposta.getData(),
                    0,
                    resposta.getLength(),
                    StandardCharsets.UTF_8
            );

            // Verifica se o servidor respondeu corretamente com a porta de upload
            if (!mensagem.startsWith("UPLOAD_PORT|")) {
                System.out.println("Erro ao iniciar upload: " + mensagem);
                return;
            }

            // Extrai a porta temporária enviada pelo servidor
            int portaTransferencia = Integer.parseInt(mensagem.split("\\|")[1]);

            System.out.println("Servidor liberou a porta " + portaTransferencia + " para upload.");
            System.out.println("Iniciando envio com Stop-and-Wait...");

            // Define o tempo máximo que o cliente esperará por um ACK
            socket.setSoTimeout(TIMEOUT);

            // Envia os pacotes do arquivo um por um
            for (int sequencia = 0; sequencia < totalPacotes; sequencia++) {

                // Calcula o início e o tamanho do trecho do arquivo
                int inicio = sequencia * TAMANHO_DADOS;
                int tamanho = Math.min(TAMANHO_DADOS, conteudo.length - inicio);

                // Copia apenas o trecho do arquivo correspondente ao pacote atual
                byte[] dadosPacote = Arrays.copyOfRange(conteudo, inicio, inicio + tamanho);

                // Cria o pacote com número de sequência, total de pacotes e dados
                byte[] pacoteBytes = criarPacote(sequencia, totalPacotes, dadosPacote);

                boolean ackRecebido = false;

                // Estratégia Stop-and-Wait:
                // envia um pacote e aguarda o ACK antes de enviar o próximo
                while (!ackRecebido) {

                    DatagramPacket pacote = new DatagramPacket(
                            pacoteBytes,
                            pacoteBytes.length,
                            enderecoServidor,
                            portaTransferencia
                    );

                    // Envia o pacote UDP para o servidor
                    socket.send(pacote);

                    System.out.println("Pacote " + sequencia + " enviado. Aguardando ACK...");

                    try {
                        byte[] bufferAck = new byte[TAMANHO_BUFFER];
                        DatagramPacket pacoteAck = new DatagramPacket(bufferAck, bufferAck.length);

                        // Aguarda o ACK do servidor
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

                        // Se o ACK não chegar dentro do tempo limite, retransmite o pacote
                        System.out.println("TIMEOUT no pacote " + sequencia + ". Retransmitindo...");
                    }
                }
            }

            // Remove o timeout para as próximas operações do cliente
            socket.setSoTimeout(0);

            System.out.println("Upload finalizado com sucesso.");

        } catch (Exception e) {
            try {
                socket.setSoTimeout(0);
            } catch (Exception ignored) {}

            System.out.println("Erro no envio do arquivo: " + e.getMessage());
        }
    }

    private static void listarArquivos(DatagramSocket socket, InetAddress enderecoServidor) {
        try {

            // Envia o comando LIST para solicitar a lista de arquivos do servidor
            enviarTexto(socket, "LIST", enderecoServidor, PORTA_SERVIDOR);

            // Aguarda a resposta do servidor
            byte[] buffer = new byte[TAMANHO_BUFFER];
            DatagramPacket resposta = new DatagramPacket(buffer, buffer.length);
            socket.receive(resposta);

            String mensagem = new String(
                    resposta.getData(),
                    0,
                    resposta.getLength(),
                    StandardCharsets.UTF_8
            );

            // Verifica se a resposta contém a lista de arquivos
            if (mensagem.startsWith("FILES|")) {
                String lista = mensagem.substring(6);

                System.out.println("\nArquivos disponíveis no servidor:");

                if (lista.equals("Nenhum arquivo disponível")) {
                    System.out.println(lista);
                } else {

                    // Separa os arquivos por vírgula e exibe um por um
                    String[] arquivos = lista.split(",");

                    for (String arquivo : arquivos) {
                        System.out.println("- " + arquivo);
                    }
                }

            } else {
                System.out.println("Resposta inesperada: " + mensagem);
            }

        } catch (Exception e) {
            System.out.println("Erro ao listar arquivos: " + e.getMessage());
        }
    }

    private static void baixarArquivo(DatagramSocket socket, InetAddress enderecoServidor, String nomeArquivo) {
        try {

            // Envia ao servidor o comando DOWNLOAD com o nome do arquivo desejado
            String comando = "DOWNLOAD|" + nomeArquivo;

            enviarTexto(socket, comando, enderecoServidor, PORTA_SERVIDOR);

            // Aguarda o servidor responder com a porta temporária e total de pacotes
            byte[] bufferResposta = new byte[TAMANHO_BUFFER];
            DatagramPacket resposta = new DatagramPacket(bufferResposta, bufferResposta.length);
            socket.receive(resposta);

            String mensagem = new String(
                    resposta.getData(),
                    0,
                    resposta.getLength(),
                    StandardCharsets.UTF_8
            );

            // Caso o servidor não encontre o arquivo
            if (mensagem.startsWith("ERRO|")) {
                System.out.println(mensagem);
                return;
            }

            // Verifica se o servidor respondeu corretamente para iniciar o download
            if (!mensagem.startsWith("DOWNLOAD_PORT|")) {
                System.out.println("Resposta inesperada do servidor: " + mensagem);
                return;
            }

            // Extrai a porta temporária e o total de pacotes
            String[] partes = mensagem.split("\\|");
            int portaTransferencia = Integer.parseInt(partes[1]);
            int totalPacotes = Integer.parseInt(partes[2]);

            System.out.println("Servidor liberou a porta " + portaTransferencia + " para download.");
            System.out.println("Total de pacotes: " + totalPacotes);

            // Informa ao servidor que o cliente está pronto para receber o arquivo
            enviarTexto(socket, "READY", enderecoServidor, portaTransferencia);

            // Map usado para guardar os pacotes recebidos pelo número de sequência
            Map<Integer, byte[]> pacotesRecebidos = new HashMap<>();

            // Objeto usado para simular perda de pacotes no download
            Random random = new Random();

            System.out.println("Iniciando download...");

            // Continua recebendo até chegar o total de pacotes esperado
            while (pacotesRecebidos.size() < totalPacotes) {

                byte[] buffer = new byte[TAMANHO_BUFFER];
                DatagramPacket pacoteRecebido = new DatagramPacket(buffer, buffer.length);

                // Recebe um pacote UDP do servidor
                socket.receive(pacoteRecebido);

                // Lê o pacote e extrai as informações
                PacoteArquivo pacote = lerPacote(
                        pacoteRecebido.getData(),
                        pacoteRecebido.getLength()
                );

                // Simula perda: ignora o pacote e não envia ACK
                if (random.nextDouble() < PERDA_PACOTES) {
                    System.out.println("[SIMULAÇÃO] Pacote " + pacote.sequencia + " ignorado. ACK não enviado.");
                    continue;
                }

                // Armazena o pacote usando o número de sequência
                pacotesRecebidos.put(pacote.sequencia, pacote.dados);

                System.out.println("Pacote " + pacote.sequencia + " recebido. Enviando ACK " + pacote.sequencia);

                // Envia ACK confirmando o recebimento do pacote
                enviarTexto(
                        socket,
                        "ACK|" + pacote.sequencia,
                        enderecoServidor,
                        portaTransferencia
                );
            }

            // Cria o arquivo final dentro da pasta downloads
            File arquivoFinal = new File(PASTA_DOWNLOADS, nomeArquivo);

            // Reconstrói o arquivo escrevendo os pacotes na ordem correta
            try (FileOutputStream fos = new FileOutputStream(arquivoFinal)) {
                for (int i = 0; i < totalPacotes; i++) {
                    fos.write(pacotesRecebidos.get(i));
                }
            }

            System.out.println("Download finalizado com sucesso.");
            System.out.println("Arquivo salvo em: " + arquivoFinal.getAbsolutePath());

        } catch (Exception e) {
            System.out.println("Erro no download: " + e.getMessage());
        }
    }

    private static void enviarTexto(DatagramSocket socket, String texto, InetAddress endereco, int porta) {
        try {

            // Converte uma mensagem de texto em bytes
            byte[] dados = texto.getBytes(StandardCharsets.UTF_8);

            // Cria o pacote UDP com a mensagem
            DatagramPacket pacote = new DatagramPacket(
                    dados,
                    dados.length,
                    endereco,
                    porta
            );

            // Envia o pacote UDP
            socket.send(pacote);

        } catch (IOException e) {
            System.out.println("Erro ao enviar mensagem: " + e.getMessage());
        }
    }

    private static int calcularTotalPacotes(long tamanhoArquivo) {

        // Calcula quantos pacotes são necessários para enviar o arquivo
        return (int) Math.ceil(tamanhoArquivo / (double) TAMANHO_DADOS);
    }

    private static byte[] lerArquivoCompleto(File arquivo) throws IOException {

        // Lê todos os bytes de um arquivo
        try (FileInputStream fis = new FileInputStream(arquivo)) {
            return fis.readAllBytes();
        }
    }

    private static byte[] criarPacote(int sequencia, int totalPacotes, byte[] dados) throws IOException {

        // Monta um pacote com cabeçalho e dados
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        // Tipo do pacote
        dos.writeUTF("DATA");

        // Número de sequência do pacote
        dos.writeInt(sequencia);

        // Total de pacotes do arquivo
        dos.writeInt(totalPacotes);

        // Tamanho dos dados deste pacote
        dos.writeInt(dados.length);

        // Dados reais do arquivo
        dos.write(dados);
        dos.flush();

        // Retorna o pacote completo em bytes
        return baos.toByteArray();
    }

    private static PacoteArquivo lerPacote(byte[] bytes, int tamanho) throws IOException {

        // Lê os bytes recebidos e extrai as informações do pacote
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes, 0, tamanho);
        DataInputStream dis = new DataInputStream(bais);

        String tipo = dis.readUTF();
        int sequencia = dis.readInt();
        int totalPacotes = dis.readInt();
        int tamanhoDados = dis.readInt();

        byte[] dados = new byte[tamanhoDados];
        dis.readFully(dados);

        // Retorna o pacote como objeto
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
