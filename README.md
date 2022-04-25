# Projeto_Semantix_Big_data
## Descrição da proposta do projeto

### Campanha Nacional de vacinação contra COVID

O projeto foi dividido em dois níveis, básico e avançado. Recomendo fortemente fazer primeiro o básico e se sobrar tempo, pode aventurar no avançado.

Os exercícios podem ser feitos em qualquer linguagem e todas as questões são bem abertas, tendo várias formas de serem realizadas e interpretadas, pois a idéia é não termos projetos iguais.

O projeto deve estar no github.com, a forma de organizar o conteúdo é por sua conta, caso nunca tenha usado, este já é seu primeiro desafio.

Ao final do projeto você precisa preencher o formulário com o seu nome completo, e-mail utilizado no treinamento e o link do github do seu projeto.

-Link do formulário para envio: https://forms.office.com/Pages/ResponsePage.aspx?id=2H_OZbilA0GZftoGjNhf1Y4a9bNsmMNEil2MBcFKJolUMFlTQVBNUVhRTVlSNVJUUDBWM0ZIRDVKQS4u

O formulário também estará na página do treinamento.

OBS: Todas as imagens de exemplo (Visualizações) são apenas para referencias, o projeto irá ter valores diferentes e as formas de se criar tabelas com dataframe/dataset das visualizações, pode ser realizado da maneira que preferir.

---
## Nível Básico

Dados:  https://mobileapps.saude.gov.br/esus-vepi/files/unAFkcaNDeXajurGB7LChj8SgQYS2ptm/04bd3419b22b9cc5c6efac2c6528100d_HIST_PAINEL_COVIDBR_06jul2021.rar

Referência das visualizações:
  - Site: https://covid.saude.gov.br
  - Guia do site: Painel Geral

---
## Preparação do ambiente de desenvolvimento

**O projeto foi realizado no ambiente Linux Ubuntu utilizando Docker e Docker-compose**

Para iniciar o projeto é necessário preparar os conteineres a serem utilizados. Para isso foi utilizado o repositório do instrutor do curso e os comandos abaixo na pasta do projeto:
```
sudo git clone https://github.com/rodrigo-reboucas/docker-bigdata.git spark
docker-compose -f docker-compose-parcial.yml pull
docker-compose -f docker-compose-parcial.yml up -d
```

Para configurar o *jar* do Spark para aceitar o formato *parquet* são necessários os comandos abaixo:
```
curl -O https://repo1.maven.org/maven2/com/twitter/parquet-hadoop-bundle/1.6.0/parquet-hadoop-bundle-1.6.0.jar
docker cp parquet-hadoop-bundle-1.6.0.jar jupyter-spark:/opt/spark/jars
```
O conteiner *namenode* foi mapeado com a pasta **input**. Logo os dados brutos do projeto a serem processados foram salvos na pasta input.

### 1. Enviar os dados para o hdfs

É necessário acessar o conteiner *namenode* para enviar. Os comandos abaixo foram utilizados para:
- Acessar conteiner *namenode*
- Criar pasta /user/projeto_semantix
- Enviar dados para pasta do *namenode* /user/projeto_semantix

```
docker exec -it namenode bash
hdfs dfs -mkdir /user/projeto_semantix
hdfs dfs -put /input/* /user/projeto_semantix
```
#### 1.1. Verificar se os arquivos brutos foram enviados para o namenode
Foi utilizado o comando ```hdfs dfs -ls /user/projeto_semantix``` e como *output* obteve-se conforme mostrado na figura abaixo:

![image](https://user-images.githubusercontent.com/84738615/165070510-cd71d90e-728b-42e4-b70c-13e9c290b9d2.png)

### 2. Otimizar todos os dados do hdfs para uma tabela Hive particionada por município

Para verificar a estrutura dos dados brutos foi utilizado o comando abaixo para visualizar as primeiras 5 linhas de um dos arquivos.

```
hdfs dfs -cat /user/projeto_semantix/HIST_PAINEL_COVIDBR_2021_Parte1_06jul2021.csv | head -5
```
![image](https://user-images.githubusercontent.com/84738615/165071716-7f1f97b6-9858-4a73-bb6f-cca92748a410.png)

Verificado a estrutura dos dados agora é conhecido de como será criado a tabela otimizada no *Hive*.

#### 2.1. Conectar-se ao Hive em outro terminal e acessar ao beeline para se conectar ao HiveServer2 utilizando-se a porta localhost:10000.
```
docker exec -it hive bash
beeline -u jdbc:hive2://localhost:10000
```
#### 2.2. Criar banco de dados

```
create database projeto_semantix;
use projeto_semantix;
```
Mostrar banco de dados **projeto_semantix** criado por meio do comando  ```show databases;```
![image](https://user-images.githubusercontent.com/84738615/165073139-2cf8da99-513d-4382-99b6-9e2297c4505f.png)

#### 2.3. Criar a tabela que irá receber os dados brutos.

![image](https://user-images.githubusercontent.com/84738615/165073629-f668362b-55bb-43e0-b1b0-8ac5b0232fed.png)

#### 2.4. Carregar dados brutos na tabela Hive

```
load data inpath '/user/projeto_semantix' into table dados_covid;
```

### 3. Criar as 3 vizualizações pelo Spark com os dados enviados para o HDFS

Acessar o Jupyter Notebook por meio da porta localhost: 8889

#### 3.1. Listar arquivos armazenados no Hive

```
!hdfs dfs -ls /user/hive/warehouse
```
![image](https://user-images.githubusercontent.com/84738615/165078386-2acda9bd-6b8b-4307-a3a1-06121582a2ea.png)

#### 3.2. Ler dados da tabela Hive e visualizar *schema*

```
tabela_dados_covid = spark.read.table("dados_covid")
tabela_dados_covid.printSchema()
```
![image](https://user-images.githubusercontent.com/84738615/165078631-e4551877-2cee-45cb-a425-b956eba5c3a0.png)

#### 3.3 Criação conforme imagem proposta na descrição do proejto

![image](https://user-images.githubusercontent.com/84738615/165080160-30930d33-a301-4e13-a31a-af2e4b18c2bc.png)

#### 3.3.1. Criação e visualização da primeira view: Casos Recuperados

```
spark.sql("CREATE OR REPLACE VIEW Total_Casos_Recuperados AS\
        SELECT MAX (recuperadosnovos) AS Casos_Recuperados, \
        MAX (emacompanhamentonovos) AS Em_Acompanhamento \
        FROM dados_covid \
        WHERE data = ('2021-07-06')")
        
view1 = spark.sql("SELECT * FROM Total_Casos_Recuperados")
view1.show()
```
![image](https://user-images.githubusercontent.com/84738615/165079197-fe9d760b-e45c-4847-9c45-2a37c9738a23.png)

#### 3.3.2. Criação e visualização da segunda view: Casos Confirmados

```
spark.sql("CREATE OR REPLACE VIEW Casos_Confirmados AS \
        SELECT MAX (casosacumulado) AS Acumulado, \
        ROUND(((MAX(casosacumulado) / MAX(populacaotcu2019))*100000),1) AS Incidencia, \
        MAX (casosnovos) AS Casos_novos \
        FROM dados_covid \
        WHERE data = ('2021-07-06')")
        
view2 = spark.sql("SELECT * FROM Casos_Confirmados")
view2.show()
```

![image](https://user-images.githubusercontent.com/84738615/165079413-6c2c97bb-6205-4cfd-91d6-1b601d61246f.png)

#### 3.3.3. Criação e visualização da terceira view: Obitos Confirmados

```
spark.sql("CREATE OR REPLACE VIEW Obitos_Confirmados AS \
        SELECT MAX (obitosacumulado) AS Obitos_acumulados, \
        MAX (casosnovos) AS Casos_novos, \
        ROUND(((MAX(obitosacumulado) / MAX(casosacumulado))*100),1) AS Letalidade, \
        ROUND(((MAX(obitosacumulado) / MAX(populacaotcu2019))*100000),1) AS Mortalidade \
        FROM dados_covid \
        WHERE data = ('2021-07-06')")
        
view3 = spark.sql("SELECT * FROM Obitos_Confirmados")
view3.show()
```

### 4. Salvar primeira visualização como tabela Hive

```
view1.write.saveAsTable("view1")
```

#### 4.1 Verificar que o arqvuivo salvo em /user/hive/warehouse

```
!hdfs dfs -ls /user/hive/warehouse/view1
```

*Saida*

![image](https://user-images.githubusercontent.com/84738615/165082388-1cf7e63e-7fb6-4bf4-bad9-22a92334a97c.png)

### 5. Salvar segunda visualização com formato parquet e compressão snappy

```
view2.write.option("compression", "snappy").parquet("/user/hive/warehouse/view2")
```

#### 5.1 Verificar que o arqvuivo salvo em /user/hive/warehouse

```
!hdfs dfs -ls /user/hive/warehouse/view2
```

*Saida*

![image](https://user-images.githubusercontent.com/84738615/165082566-d5b9dd34-695b-4cf4-9fd6-9a6a42f2fbff.png)

### 6. Salvar a terceira visualização em um tópico no Káfka

#### 6.1. Acessar conteiner Kafka e criar tópico

Em um outro terminal executar os comandos abaixo:
```
docker exec -it kafka bash
kafka-topics.sh --bootstrap-server kafka:9092 --topic view3 --create --partitions 1 --replication-factor 1
```

#### 6.2. Verificar tópico criado

```
kafka-topics.sh --bootstrap-server kafka:9092 --list
```

#### 6.4. Instruir tópico criado para ser um consumer

kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic view3

#### 6.4. Salvar view

No Jupyter Notebook executar os comandos abaixo:

```
from pyspark.sql.functions import *
from pyspark.sql.types import *

view3_convertido = view3.withColumn(
            "value", struct(
            col("Obitos_acumulados"), \
            col("Casos_novos"), \
            col("Letalidade"), \
            col("Mortalidade")))
view3_convertido = view3_convertido.withColumn("value",col("value").cast("string"))

view3_convertido.show()

view3_convertido.write.format("kafka")
        \.option("kafka.bootstrap.servers", "kafka:9092")
        \.option("topic","view3").save()
```

#### 6.5. Verificar a terceira visualização no Kafka

![image](https://user-images.githubusercontent.com/84738615/165084554-dde58790-39e0-4427-8081-598d69413345.png)

### 7. Criar a visualição pelo Spark com os dados enviados para o HDFS:

![image](https://user-images.githubusercontent.com/84738615/165084955-6d1c6f1b-f00b-40f5-bf94-32a671178327.png)

#### 7.1 No Jupyter Notebook executar os comandos abaixo:

```
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

view_spark = tabela_dados_covid.select(
    'regiao',\
    'populacaotcu2019',\
    'obitosacumulado',\
    'casosacumulado',\
    'estado').where(tabela_dados_covid.regiao != 'regiao')

view_spark = view_spark.groupBy('estado', 'regiao')
view_spark = view_spark.max('casosacumulado','obitosacumulado','populacaotcu2019')

view_spark = view_spark.groupBy('regiao')
view_spark = view_spark.sum('max(casosacumulado)','max(obitosacumulado)','max(populacaotcu2019)')
view_spark = view_spark.withColumn('mult', lit(100000))

view_spark = view_spark.withColumn('Incidência/mil hab.', \
        format_number(((col('sum(max(casosacumulado))') * col('mult'))/col('sum(max(populacaotcu2019))')),1))
view_spark = view_spark.withColumn('Mortalidade/mil hab.', \
        format_number(((col('sum(max(obitosacumulado))')/col('sum(max(populacaotcu2019))'))) * col('mult'),1))

view_spark = view_spark.drop('mult','sum(max(populacaotcu2019))')
view_spark = view_spark.withColumnRenamed('sum(max(casosacumulado))', 'Casos').\
        withColumnRenamed('sum(max(obitosacumulado))', 'Óbitos').\
        withColumnRenamed('regiao', 'Região')
```
#### 7.2 Verificar visualização

```
view_spark.toPandas()
```

*Saida*

![image](https://user-images.githubusercontent.com/84738615/165085330-64b8f703-6163-4f77-8ccc-6bfc2fc0a607.png)

### 8. Salvar a visualização do exercício 6 em um tópico no Elastic

```
view3_el = view3.write.format('csv')\
    .option("inferSchema", "true")\
    .option("header","true")\
    .save("/user/hive/warehouse/elastic_search/view3_elastic.csv")

!hdfs dfs -ls /user/hive/warehouse/elastic_search/view3_elastic.csv

spark.read.csv('/user/hive/warehouse/elastic_search/view3_elastic.csv', inferSchema = True, header = True).toPandas()
```

![image](https://user-images.githubusercontent.com/84738615/165099817-86ae4cbb-2659-4b8e-9316-30951c0a1591.png)

### 9 Criar um dashboard no Elastic para visualização dos novos dados enviados

O importação de dados para o acesso gratuito ao Kibana é limitado a 100 Mb. Portanto foi importado somente o arquivo: 
HIST_PAINEL_COVIDBR_2020_Parte1_06jul2021.csv  HIST_PAINEL_COVIDBR_2021_Parte1_06jul2021.csv

Feito a importação foi possível a criação de um dashboard.

Para efeitos de teste foi criado um gráfico de pizza contendo os maiores valores de obitos ordenados pelos estados com maior número de obitos.

![image](https://user-images.githubusercontent.com/84738615/165121577-c39958ec-bcd2-40a3-8c9d-17cb6fbeed5d.png)
