#Baixando a base de dados para a variável x

import pandas as pd
x = pd.read_csv(
    filepath_or_buffer='https://noverde-data-engineering-test.s3.amazonaws.com/dataeng_test.csv',
    sep=',')

#Excluindo a coluna que apresenta apenas valores nulos e passando os dados para a variável df

df = x.drop('Unnamed: 4', axis=1)

#Tratando a coluna 'ValorPago' - Removendo primeiros os '.'

df['ValorPago'] = df['ValorPago'].apply(lambda x: x.replace('.', ''))

#Tratando a coluna 'ValorPago' - Removendo as vírgulas e substituindo para pontos

df['ValorPago'] = df['ValorPago'].apply(lambda x: x.replace(',', '.'))

#Transformando a coluna 'ValorPago' em float

df['ValorPago']=df['ValorPago'].astype('float64')

#Transformando a coluna 'Comarca' - Removendo ' / SP'

df['Comarca'] = df['Comarca'].apply(lambda x:x.replace(' / SP',''))

#Baixando o CSV

df.to_csv('C:\Files\protestos.csv',index=False)

#Importando o adaptador do PostgreSQL para python
import psycopg2

#Criando a conexão com o PostgreSQL
conex = psycopg2.connect(host='localhost', database='Noverde',
user='postgres', password='*****')

#Abrindo o cursor para criação da tabela "payments"
cur = conex.cursor()

#Criando a tabela "payments"

cur.execute("CREATE TABLE payments (id serial PRIMARY KEY, payment_date date, amount decimal(10,2),protocol varchar(7),city varchar(30), state varchar(2) DEFAULT 'sp');")

# Inserindo dados na tabela "payments"

cur.execute("COPY payments (protocol,city,payment_date,amount) FROM 'C:\Files\protestos.csv' DELIMITER ',' CSV HEADER;")

# Query 1 
#Qual foi o valor recebido em todo o período?

cur.execute("SELECT cast(sum(amount) as money) as ValorRecebido FROM payments;")
cur.fetchone()

#Query 2
#Qual o maior e o menor valor recebido em 2019?

cur.execute("SELECT cast(max(amount) as money) as MaiorValor, cast(min(amount) as money) as MenorValor FROM payments WHERE payment_date BETWEEN '20190101' AND '20191231';")
cur.fetchall()

#Query 3
#Qual o mês que teve o maior recebimento?

cur.execute("SELECT EXTRACT('YEAR' FROM payment_date) AS Ano, EXTRACT('MONTH' FROM payment_date) AS Mes, CAST(sum(amount)AS MONEY) AS Valortotal FROM payments GROUP BY EXTRACT('MONTH' FROM payment_date), EXTRACT('YEAR' FROM payment_date) ORDER BY Valortotal desc limit 1;")
cur.fetchall()

#Query 4
#Qual a comarca que teve mais recebimento?

cur.execute("SELECT city AS Comarca, CAST(sum(amount)AS MONEY) AS Valor FROM payments GROUP BY city ORDER BY Valor desc limit 1;")
cur.fetchall()

#Query 5
# Qual a porcentagem que cada mês de 2019 teve no montante recebido de 2019? 

cur.execute("SELECT EXTRACT('MONTH' from payment_date) AS month, EXTRACT('YEAR' from payment_date) AS year, cast(sum(amount)as money) as amount, cast(sum(amount)/(SELECT sum(amount) from payments WHERE EXTRACT('YEAR' from payment_date) = 2019)*100 as decimal(10,2)) as Ratio FROM payments WHERE EXTRACT('YEAR' from payment_date) = 2019 GROUP BY EXTRACT('MONTH' FROM payment_date), EXTRACT('YEAR' from payment_date) ORDER BY month asc;")
cur.fetchall()

#Fazendo as alterações no banco

conex.commit ()

#Fechando cursor e conexão

cur.close ()
conex.close ()

#Fim
