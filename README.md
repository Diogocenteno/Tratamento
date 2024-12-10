Este código realiza várias etapas de processamento de dados usando PySpark, com o objetivo de limpar, transformar e salvar conjuntos de dados provenientes de arquivos CSV em um formato mais estruturado. Aqui está um resumo detalhado de cada parte do código:

Processamento e Transformação de Dados de Vídeos e Comentários com PySpark

1. Importação de Bibliotecas e Inicialização da Spark Session:
PySpark: A biblioteca necessária para o processamento distribuído.
SparkSession: Usada para criar a sessão de Spark.
O código começa importando as bibliotecas necessárias, como funções de transformação (col, year) e tipos de dados (IntegerType, DateType), e inicializa a Spark Session.
2. Leitura e Validação do Arquivo videos-stats.csv:
Lê o arquivo videos-stats.csv usando spark.read.csv().
Exibe os primeiros 10 registros e o esquema do DataFrame para validação.
3. Tratamento de Valores Nulos:
Preenche os valores nulos nas colunas Likes, Comments e Views com 0.
Exibe amostra dos dados após o preenchimento.
4. Leitura e Validação do Arquivo comments.csv:
Lê o arquivo comments.csv e exibe os primeiros registros e o esquema.
5. Contagem de Registros:
Conta o número de registros em ambos os DataFrames (df_video e df_comentario) antes e depois de realizar transformações (como remoção de valores nulos).
6. Remoção de Registros com Video ID Nulo:
Remove registros onde o campo Video ID é nulo, aplicando um filtro isNotNull().
7. Remoção de Duplicatas no Campo Video ID:
Remove registros duplicados no campo Video ID no DataFrame df_video.
8. Conversão de Tipos para Inteiros:
Converte as colunas Likes, Comments e Views para o tipo IntegerType.
9. Conversão e Renomeação de Colunas no df_comentario:
Converte as colunas Likes e Sentiment para inteiros e renomeia Likes para Likes Comment.
10. Criação da Coluna Interaction:
Cria uma nova coluna Interaction no DataFrame df_video, que é a soma de Likes, Comments e Views.
11. Conversão de Published At para Tipo Data e Criação da Coluna Year:
Converte a coluna Published At para o tipo DateType e extrai o ano dessa data, criando a coluna Year.
12. Mesclagem de df_comentario no df_video:
Realiza uma junção (join) entre df_video e df_comentario com base no campo Video ID.
13. Leitura e Mesclagem do Arquivo USvideos.csv:
Lê o arquivo USvideos.csv e faz uma junção com o DataFrame df_video usando a coluna Title.
14. Verificação de Valores Nulos em df_video:
Verifica a quantidade de valores nulos em cada coluna de df_video e exibe o resultado.
15. Salvamento de DataFrames no Formato Parquet:
Remove a coluna _c0, caso exista, e salva o DataFrame resultante (df_video e df_join_video_comments) em formato Parquet.
Os DataFrames são salvos em diretórios específicos (../data/videos-tratados-parquet e ../data/videos-comments-tratados-parquet).
16. Encerramento da Spark Session:
Finaliza a Spark Session, liberando recursos utilizados durante o processamento.
Considerações:
PySpark é utilizado para processar dados grandes de forma distribuída, o que permite que você trabalhe com conjuntos de dados que não cabem na memória de um único computador.
O código realiza limpeza de dados, transformação de tipos e integração de múltiplos conjuntos de dados através de junções, com o objetivo final de gerar DataFrames tratados e salvos em formato Parquet para uso posterior.
