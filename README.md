# Desafio Raizen - Data Engineer
By Silvio Martins (scesar.martins#gmail.com) in 21Abr2022
## Objetivos:
Desenvolver um pipeline de dados, orquestrado, que extraia informações de uma tabela dinâmica do site da ANP e coloque-as em um repositório no formato particionado ou indexado.
## Tecnologias Usadas
1. Apache Airflow - Orquestrador de código aberto da Apache, totalmente manipulado em linguagem Python, onde será construído uma DAG que comportará todo nosso pipeline da solução;
2. Linguagem Python - atualmente uma das principais linguagens aplicada em dados, onde desenvoleremos as rotinas de extração, transformação e carga dos dados. Em nossa solução procuramos usar a melhores práticas de desenvolvimento bem como procuramos usar recursos de Orientação a Objetos, nativo da linguagem Python;
3. Docker - Utilização da tecnologia de conteinerização com uso do Docker, onde criaremos nosso ambiente airflow de forma simples e fácil manipulação através da ferramenta docker-compose, permitindo ter uma aplicação escalável e tolerante a falha visto que lançaremos mão de mais de um worker que fará a diferença em ambiente clusterizado;
4. Data Lake - Conceito de armazenamento de dados bem difundido em big-data e em soluções de BI, onde trabalharemos as entregas da solução em uma área de armazenamento divida em 3 sub-áreas: a) A Bronze_Zone - diretório de entrada dos dados onde colocaremos os dados extraídos da fonte em questão. Lá teremos a cópia fiel da origem do dado. b) A Silver_Zone - Aqui colocaremos o dado após a conversão do documento para .xlsx, com algum tipo de tratamento. E c) a Gold_Zone - Aqui colocaremos o dado tratado com as regras aplicadas bem como armezado em formato binário Parquet, Parquet, com uso da compressão SNAPPY e Particonado , garantindo assim uma otimização no tamnanho do dado e nas rotinas de consulta.
5. LibreOffice Headless - Aqui utilizaremos a versão do LibreOffice no modo Headless, compacta, onde utilizaremos o mesmo para a conversão do arquivo .xls para .xlsx. Foi a solução mais adequado, pois na conversão o arquivo final nos é apresentado em sua íntegra com os dados de cache no formato de tabelas. O mesmo será usado tbm no formato docker, através do recurso docker run. 
## Solução:
1. A Extração - O airflow em seu pipeline chamará o código python que se conectará no site da ANP e via requests fará o dawnload da planilha no formato .xls, armazenando-o na Bronze_Zone;
2. A Transformação - Aqui aplicaremos a solução LibreOffice de forma headless, via docker run para efeturmos a conversão do arquivo em .xlsx e colocaremos o mesmo na área Silver_Zone do nosso data lake, onde na sequência faremos o tratamento dos referidos dados com a ajuda do pandas do python, criando os dados na forma correta como solicitado;
3. A Carga - Com os dados já tratados e padronizados no formato correto, salvaremos no nosso data lake, na área Gold_Zone, em formato Parquet, com compressão SNAPPY e particionado por UF e PRODUTOU;
4. Rotinas de Checagem - Foi implementado no código um ponto de testagem com o objetivo de comparar os dados da origem com os dados armazenados na tabela parquet, de forma a garantir a qualidade dos dados.
## O Ambiente
1. Arquivo DockerFile - O arquivo dockerfile terá a rotina de customização da imagem do airflow 2.2.3, onde criaremos nova imagem adaptada à nossa solução e será usado em docker;
2. Arquivo docker-compose.yaml - Arquivo que consta as instruções de montagem do nosso ambiente airflow containerizado;
3. Arquivo start.sh - Script Linux, responsável criar a imagem customizada do airflow, subir o ambiente para uso com 3 workers, garantindo um ambiente conteinerizado, escalável e tolerante a falha.
## Subindo o Ambiente
Executar os passos abaixo em ambiente Linux. exceutei a solução no WSL2:
1. Criar o diretório airflow em home: 
    mkdir airfow
2. Copiar os arquivos dockerfile, docker-compose.yaml, start.sh e todo diretório dags, deste repositório, para o diretório airflow criado;
3. Acessar a pasta airflow e executar o arquivo star.sh
    sudo . /start.sh
4. Nesse ponto acessar o container do web-server do airflow usando docker exec -it <id_container> bash, e instalar a biblioteca python fastparquet usando pip install fastparquet; 
5. Ir no navegador e acessar a interfaxe web do airflow através do endereço:
        http://localhost:8080
6. Acessar com USUÁRIO: airflow e SENHA: airflow, ir em DAGS, clicar em DAG´s e na DAG ETL_raizen e executá-la;
7. Aguardar o fim do processamento e verificar as saídas geradas no data lake.
        ./dags/data_lake
8. Finalize o ambiente com docker-compose stop, para finalizar os containers

## Pontos de Melhoria
1. Tratar o apontamento do data lake através de um arquivo .xml, tornando o processo mais flexível;
2. Implementar a rotina de processamento em ambiente Spark (PySpark), melhorando o tempo de processamento;
3. Corrigir a configuração do envio de email em caso de falha nas tasks.
## Conclusões:
O trabalho de automação de tabelas dinâmicas é bem desafiador pois econtramos soluções escassas para realizar tais processamentos.