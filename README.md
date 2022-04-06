# Desafio Raizen - Data Engineer
By Silvio Martins (scesar.martins#gmail.com) in 06Abr2022
## Objetivos:
Desenvolver um pipeline de dados, orquestrado, que extraia informações de uma Pivot table da ANP e coloque essas informações em um repositório no formato particionado ou indexado.
## Solução:
Uso do airflow como orquestrador do fluxo, de forma containerizada, scripts python com uso de OO para realização dos processamentos, dockerOperator do airflow para transformação do xls em xlsx
## Conclusões:
O trabalho de automação de tabelas dinâmicas é bem desafiador pois econtramos soluções escassas para realizar tais processamentos.