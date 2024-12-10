#%% Importação de bibliotecas
# Importa as bibliotecas necessárias para o processamento com PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year
from pyspark.sql.types import IntegerType, DateType

#%% Inicializar a Spark Session
# Cria uma sessão Spark para realizar o processamento dos dados
spark = SparkSession.builder \
    .appName("YouTube Data Processing") \
    .getOrCreate()

#%% Etapa 1: Ler o arquivo 'videos-stats.csv'
# Lê o arquivo 'videos-stats.csv' e carrega no DataFrame 'df_video'
# A opção header=True considera a primeira linha como cabeçalho
# A opção inferSchema=True infere automaticamente os tipos de dados das colunas
df_video = spark.read.csv("../data/videos-stats.csv", header=True, inferSchema=True)

# Exibe os primeiros registros do DataFrame para validação
print("Primeiros registros de 'videos-stats.csv':")
df_video.show(10, truncate=False)

# Exibe o esquema do DataFrame (nomes e tipos de colunas)
print("Esquema de 'videos-stats.csv':")
df_video.printSchema()

#%% Etapa 2: Preencher valores nulos
# Substitui valores nulos nos campos 'Likes', 'Comments' e 'Views' por 0
df_video = df_video.fillna({'Likes': 0, 'Comments': 0, 'Views': 0})

# Exibe os dados após o preenchimento de valores nulos
print("Dados após preencher valores nulos (amostra):")
df_video.show(10, truncate=False)

#%% Etapa 3: Ler o arquivo 'comments.csv'
# Lê o arquivo 'comments.csv' e carrega no DataFrame 'df_comentario'
# As opções header=True e inferSchema=True também são utilizadas aqui
df_comentario = spark.read.csv("../data/comments.csv", header=True, inferSchema=True)

# Exibe os primeiros registros do DataFrame para validação
print("Primeiros registros de 'comments.csv':")
df_comentario.show(10, truncate=False)

# Exibe o esquema do DataFrame (nomes e tipos de colunas)
print("Esquema de 'comments.csv':")
df_comentario.printSchema()

#%% Etapa 4: Calcular a quantidade de registros
# Conta e exibe o número de registros em 'df_video' e 'df_comentario'
count_video = df_video.count()
count_comentario = df_comentario.count()
print(f"Registros iniciais - df_video: {count_video}, df_comentario: {count_comentario}")

#%% Etapa 5: Remover registros com 'Video ID' nulo
# Remove registros onde o campo 'Video ID' é nulo em ambos os DataFrames
df_video = df_video.filter(df_video["Video ID"].isNotNull())
df_comentario = df_comentario.filter(df_comentario["Video ID"].isNotNull())

# Conta novamente os registros e exibe os resultados
count_video = df_video.count()
count_comentario = df_comentario.count()
print(f"Registros após remoção de nulos - df_video: {count_video}, df_comentario: {count_comentario}")

#%% Etapa 6: Remover duplicatas no campo 'Video ID' em df_video
# Remove registros duplicados no campo 'Video ID' do DataFrame 'df_video'
df_video = df_video.dropDuplicates(["Video ID"])

# Exibe uma amostra dos dados após a remoção de duplicatas
print("Dados após remover duplicatas (amostra):")
df_video.show(15, truncate=False)

#%% Etapa 7: Converter colunas para tipos inteiros no DataFrame 'df_video'
# Converte as colunas 'Likes', 'Comments' e 'Views' para o tipo inteiro
df_video = df_video.withColumn("Likes", col("Likes").cast(IntegerType())) \
                   .withColumn("Comments", col("Comments").cast(IntegerType())) \
                   .withColumn("Views", col("Views").cast(IntegerType()))

# Exibe uma amostra dos dados após a conversão de tipos
print("Dados após conversão de tipos (amostra):")
df_video.show(15, truncate=False)

#%% Etapa 8: Converter e renomear colunas no DataFrame 'df_comentario'
# Converte as colunas 'Likes' e 'Sentiment' para o tipo inteiro
# Renomeia a coluna 'Likes' para 'Likes Comment'
df_comentario = df_comentario.withColumn("Likes", col("Likes").cast(IntegerType())) \
                             .withColumn("Sentiment", col("Sentiment").cast(IntegerType())) \
                             .withColumnRenamed("Likes", "Likes Comment")

# Exibe uma amostra dos dados após a conversão e renomeação
print("Dados de comentários após conversão e renomeação (amostra):")
df_comentario.show(15, truncate=False)

#%% Etapa 9: Criar a coluna 'Interaction' em df_video
# Cria a nova coluna 'Interaction' como a soma de 'Likes', 'Comments' e 'Views'
df_video = df_video.withColumn("Interaction", col("Likes") + col("Comments") + col("Views"))

# Exibe uma amostra dos dados após adicionar a coluna 'Interaction'
print("Dados após criar a coluna 'Interaction' (amostra):")
df_video.show(15, truncate=False)

#%% Etapa 10: Converter 'Published At' para tipo data e criar a coluna 'Year'
# Converte o campo 'Published At' para o tipo data
# Cria a nova coluna 'Year', extraindo o ano do campo 'Published At'
df_video = df_video.withColumn("Published At", col("Published At").cast(DateType())) \
                   .withColumn("Year", year(col("Published At")))

# Exibe uma amostra dos dados após a conversão de data e criação de 'Year'
print("Dados após conversão de 'Published At' e criação de 'Year' (amostra):")
df_video.show(15, truncate=False)

#%% Etapa 11: Mesclar df_comentario no DataFrame df_video
# Realiza uma junção (join) entre 'df_comentario' e 'df_video' com base na coluna 'Video ID'
df_join_video_comments = df_video.join(df_comentario, on="Video ID", how="left")

# Exibe uma amostra dos dados após a junção
print("Dados após junção com comentários (amostra):")
df_join_video_comments.show(15, truncate=False)

#%% Etapa 12: Ler o arquivo 'USvideos.csv'
# Lê o arquivo 'USvideos.csv' e carrega no DataFrame 'df_us_videos'
df_us_videos = spark.read.csv("../data/USvideos.csv", header=True, inferSchema=True)

# Exibe os primeiros registros e o esquema do DataFrame
print("Primeiros registros de 'USvideos.csv':")
df_us_videos.show(15, truncate=False)
print("Esquema de 'USvideos.csv':")
df_us_videos.printSchema()

#%% Etapa 13: Mesclar df_us_videos no DataFrame df_video
# Realiza uma junção (join) entre 'df_us_videos' e 'df_video' com base na coluna 'Title'
df_join_video_usvideos = df_video.join(df_us_videos, on="Title", how="inner")

# Exibe uma amostra dos dados após a junção
print("Dados após junção com USvideos (amostra):")
df_join_video_usvideos.show(15, truncate=False)

#%% Etapa 14: Verificar a quantidade de valores nulos em df_video
# Verifica a quantidade de valores nulos em cada coluna de 'df_video'
print("Quantidade de valores nulos em cada coluna de 'df_video':")
df_video.select([col(c).isNull().alias(c) for c in df_video.columns]).groupBy().sum().show(truncate=False)

#%% Etapa 15: Salvar df_video no formato Parquet
# Remove a coluna '_c0', se existir, e salva o DataFrame no formato Parquet
df_video = df_video.drop("_c0")
df_video.write.mode("overwrite").parquet("../data/videos-tratados-parquet")
print("DataFrame 'df_video' salvo no formato Parquet em '../data/videos-tratados-parquet'.")

#%% Etapa 16: Salvar df_join_video_comments no formato Parquet
# Remove a coluna '_c0', se existir, e salva o DataFrame no formato Parquet
df_join_video_comments = df_join_video_comments.drop("_c0")
df_join_video_comments.write.mode("overwrite").parquet("../data/videos-comments-tratados-parquet")
print("DataFrame 'df_join_video_comments' salvo no formato Parquet em '../data/videos-comments-tratados-parquet'.")

#%% Encerrar Spark Session
# Finaliza a Spark Session para liberar os recursos
spark.stop()
print("Spark Session encerrada.")


#%%