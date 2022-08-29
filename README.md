# Análise Fundamentalista

Este é um projeto cujo objetivo principal é o desenvolvimento de uma aplicação capaz de fornecer o acesso a informação de empresas listadas na BMFBovespa para a realização de análises fundamentalistas.

Os códigos têm a finalidade de realizar o provisionamento da infraestrutura necessária para armazenamento e execução das tarefas. Em um primeiro momento, as tarefas de extração de dados e pré-processamento serão realizadas localmente, no entanto, em um segundo momento, os códigos serão adaptados para funcionar 100% em cloud. A imagem a seguir apresenta o fluxo da aplicação de dados:


# Estrutura dos Dados

![Pipeline de Dados](./application-flow.png)


## **Camada raw** 

A primeira etapa consiste na extração dos dados de empresas disponíveis na CVM e persistência deles na camada **Raw** do datalake na AWS. Essa camada armazena o dado da forma mais bruta, ou seja, o dado é extraido e armazenado sem ser feito nenhum tipo de alteração.


## **Camada pre_processed**

### Taxonomia da Nomenclatura dos Arquivos Persistidos

Na segunda etapa, os dados são pré-processados e armazenados na camada **pre_processed** do datalake. 

<font size="3">**Nomenclatura Arquivos = pp_periodo_tipo**</font>

Onde: 

Taxonomia | Descrição 
------|------
pp | Indica que os arquivos são pré-processados.
Período | Indica a janelam temporal da variável.
Tipo | Diz respeito ao tipo do documento disponibilizado pela CVM.

**Período**
Abreviação | Descrição 
------|------
ITR | Formulário de Informações Trimestrais

**Tipo**
Abreviação | Descrição 
------|------
DRE | Demonstração de Resultado
BPA | Balanço Patrimonial Ativo
BPP | Balanço Patrimonial Passivo
DVA | Demonstração de Valor Adicionado
DRA | Demonstração de Resultado Abrangente
DMPL | Demonstração das Mutações do Patrimônio Líquido
DFC_MD | Demonstração de Fluxo de Caixa - Método Direto
DFC_MI | Demonstração de Fluxo de Caixa - Método Indireto


### Taxonomia da Nomenclatura das Variáveis

Nesta etapa os atributos relevantes são selecionados, renomeados (com base nas regras de taxonomia) e transformados.

A nomenclatura das variáveis são construídas com base na seguinte taxonomia:

<font size="3">**Nomenclatura Variáveis = tipo_tema**</font>

Onde: 

Taxonomia | Descrição 
------|------
Tipo | Indica o tipo da variável.
Tema | Diz respeito a natureza do atributo, ou seja, o que ela de fato representa.

**Tipo**
Abreviação | Descrição 
------|------
id | Representa variáveis de identificação.
txt | Texto.
dt | Variável de data.
cat | Indica uma variável categorica.
amt | Indica um montante financeiro R$.
qty | Indica quantidade.
pct | Percentual.
is | Representa uma variável binária 1 (True) ou 0 (False).

**Tema**
Nomenclatura que indica o que de fato é o dado.


## Camada analytical

### Taxonomia das Variáveis

A nomenclatura das variáveis são construídas com base na seguinte taxonomia:

<font size="3">**Taxonomia = tipo_medida_tema_periodo**</font>

Onde: 

Taxonomia | Descrição 
------|------
Tipo | Indica o tipo da variável.
Medida | Nos casos onde a variável representa uma medida de resumo, indica qual a medida.
Tema | Diz respeito a natureza do atributo, ou seja, o que ela de fato representa.
Período | Quando se aplica, indica a janela utilizada para mensurar a respectiva medida.

**Tipo**
Abreviação | Descrição 
------|------
id | Representa variáveis de identificação.
dt | Variável de data.
cat | Indica uma variável categorica.
amt | Indica um montante financeiro R$.
qty | Indica quantidade.
pct | Percentual.
is | Representa uma variável binária 1 (True) ou 0 (False).

**Medida**
Abreviação | Descrição 
------|------
avg | Média
mda | Mediana
std | Desvio Padrão
min | Mínimo
max | Máximo
tot | total

**Período**
Abreviação | Descrição 
------|------
1m | Indica uma janela de 1 (n) dia.
1m | Indica uma janela de 1 (n) mês.
1q | Indica uma janela de 1 (n) trimestre.
1s | Indica uma janela de 1 (n) semestre.
1s | Indica uma janela de 1 (n) ano.


# Estrutura da Aplicação

### Utils

- extraction_cvm
- unzippded_files
- saving_raw_data
- load_bucket


### PreProcessing

- _pre_processing_itr_dre
- _pre_processing_itr_bpp
- _pre_processing_itr_bpa
- pre_process_itr_dre



## Dicionário de Dados Brutos CVM

**Documentos: Formulário de Demonstrações Financeiras Padronizadas (DFP)**

O Formulário de Demonstrações Financeiras Padronizadas (DFP) é formado por um conjunto de documentos encaminhados periodicamente devido a normativa 480/09 da CVM.

**Formulário de Informações Trimestrais (ITR)**

O ITR é semlhante ao DFP, exeto pelo fato de conter informações contáveis trimestrais.

#### Documentos
* Balanço Patrimonial Ativo (BPA)
* Balanço Patrimonial Passivo (BPP)
* Demonstração de Fluxo de Caixa - Método Direto (DFC-MD)
* Demonstração de Fluxo de Caixa - Método Indireto (DFC-MI)
* Demonstração das Mutações do Patrimônio Líquido (DMPL)
* Demonstração de Resultado Abrangente (DRA)
* Demonstração de Resultado (DRE)
    * dfp_cia_aberta_2011.zip
        * dfp_cia_aberta_DRE_con_2022.csv - Consolidada
        * dfp_cia_aberta_DRE_ind_2022.csv - Individual

* Demonstração de Valor Adicionado (DVA)

*Obs*:
Consolidado: É referente aos dados do grupo econômico pela qual a empresa faz parte.
Individual: Diz respeito a empresa que é a controladora de um grupo individual


#### Links

* Dados Disponíves: http://dados.cvm.gov.br/

* Dados Disponíves DFP: https://dados.cvm.gov.br/dataset/cia_aberta-doc-dfp

* Dados Disponíves ITR: https://dados.cvm.gov.br/dataset/cia_aberta-doc-itr

* Dicionário dos Dados ITR: http://dados.cvm.gov.br/dataset/cia_aberta-doc-itr/resource/062b8f02-ca6b-424a-bf65-180ff2b69af2