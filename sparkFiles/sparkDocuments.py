import os
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType


PATH_DATALAKE = '/datalake'
DIR_PATH_RAW_DFP = os.path.join(PATH_DATALAKE, 'raw-dfp')
DIR_PATH_RAW_ITR = os.path.join(PATH_DATALAKE, 'raw-itr')
DIR_PATH_RAW_STOCK = os.path.join(PATH_DATALAKE, 'raw-stock')
DIR_PATH_PROCESSED_DFP = os.path.join(PATH_DATALAKE, 'pre-processed-dfp')
DIR_PATH_PROCESSED_ITR = os.path.join(PATH_DATALAKE, 'pre-processed-itr')

DIR_S3_RAW_DFP = 's3a://fundamentus-pre-processed-dfp'
DIR_S3_RAW_ITR = 's3a://fundamentus-pre-processed-itr'
DIR_S3_ANALYTICAL_DRE = 's3a://fundamentus-analytical-dre'

types_dict = {
            'itr_dre':'itr_cia_aberta_DRE_con',
            'itr_bpp':'itr_cia_aberta_BPP_con',
            'itr_bpa':'itr_cia_aberta_BPA_con',
            'dfp_dre':'dfp_cia_aberta_DRE_con',
            'dfp_bpp':'itr_cia_aberta_BPP_con',
            'dfp_bpa':'dfp_cia_aberta_BPA_con'
        }


# Auxiliary codes
bpp_account = {
            '2':'amt_total_liabilities',
            '2.01':'amt_current_liabilities',
            '2.02':'amt_non_current_liabilities',
            '2.02.01':'amt_loans_credits',
            '2.03':'amt_net_equity'
}

bpa_account = {
            '1':'total_assets',
            '1.01':'current_assets',
            '1.01.01':'cash_and_cash_equivalents',
            '1.01.04':'stocks',
            '1.02':'non_current_assets'
}

dre_account = {
            '3.01':'sales_revenue',
            '3.02':'cost_goods_and_services',
            '3.03':'groos_revenue',
            '3.04':'operating_revenues_and_expenses',
            '3.05':'earnings_before_interest_and_taxes',
            '3.06':'financial_results',
            '3.07':'earnings_before_income_tax_and_social_contribution',
            '3.11':'net_profit'
}


schema_dre = StructType([
    StructField('CNPJ_CIA', StringType(), True),
    StructField('DT_REFER', StringType(), True),
    StructField('VERSAO', IntegerType(), True),
    StructField('DENOM_CIA', StringType(), True),
    StructField('CD_CVM', StringType(), True),
    StructField('GRUPO_DFP', StringType(), True),
    StructField('MOEDA', StringType(), True),
    StructField('ESCALA_MOEDA', StringType(), True),
    StructField('ORDEM_EXERC', StringType(), True),
    StructField('DT_INI_EXERC', StringType(), True),
    StructField('DT_FIM_EXERC', StringType(), True),
    StructField('CD_CONTA', StringType(), True),
    StructField('DS_CONTA', StringType(), True),
    StructField('VL_CONTA', FloatType(), True),
    StructField('ST_CONTA_FIXA', StringType(), True)
])

schema_bp_ba = StructType([
    StructField('CNPJ_CIA', StringType(), True),
    StructField('DT_REFER', StringType(), True),
    StructField('VERSAO', IntegerType(), True),
    StructField('DENOM_CIA', StringType(), True),
    StructField('CD_CVM', StringType(), True),
    StructField('GRUPO_DFP', StringType(), True),
    StructField('MOEDA', StringType(), True),
    StructField('ESCALA_MOEDA', StringType(), True),
    StructField('ORDEM_EXERC', StringType(), True),
    StructField('DT_FIM_EXERC', StringType(), True),
    StructField('CD_CONTA', StringType(), True),
    StructField('DS_CONTA', StringType(), True),
    StructField('VL_CONTA', FloatType(), True),
    StructField('ST_CONTA_FIXA', StringType(), True)
])

schema_ticker = StructType([
    StructField('variable_type', StringType(), True),
    StructField('ticker', StringType(), True),
    StructField('value', FloatType(), True)
])

schema_ticker2 = StructType([
    StructField('ticker', StringType(), True),
    StructField('date', DateType(), True),
    StructField('open', FloatType(), True),
    StructField('high', FloatType(), True),
    StructField('low', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('adj_close', FloatType(), True),
    StructField('volume', IntegerType(), True)
])

# Create an empty RDD with empty schema
schema_pp_dre = StructType([
    StructField('id_cvm', StringType(), True),
    StructField('id_cnpj', StringType(), True),
    StructField('txt_company_name', StringType(), True),
    StructField('dt_refer', DateType(), True),
    StructField('dt_fim_exerc', DateType(), True),
    StructField('dt_ini_exerc', DateType(), True),
    StructField('dt_year', IntegerType(), True),
    StructField('dt_quarter', IntegerType(), True),
    StructField('cost_goods_and_services', FloatType(), True),
    StructField('earnings_before_income_tax_and_social_contribution',
                FloatType(), True),
    StructField('earnings_before_interest_and_taxes', FloatType(), True),
    StructField('financial_results', FloatType(), True),
    StructField('groos_revenue', FloatType(), True),
    StructField('net_profit', FloatType(), True),
    StructField('operating_revenues_and_expenses', FloatType(), True),
    StructField('sales_revenue', FloatType(), True),
    StructField('processed_at', DateType(), True),
])

ticker_list = ['ABCB4.SA', 'AGRO3.SA', 'RAIL3.SA', 'ALPA3.SA', 'ALPA4.SA', 'ALSO3.SA', 'AMAR3.SA', 'ABEV3.SA', 'ADHM3.SA', 'ARZZ3.SA', 'BBAS3.SA', 'BBDC3.SA', 'BBDC4.SA',
'BEEF3.SA', 'BPHA3.SA', 'BPAN4.SA', 'BRAP3.SA', 'BRAP4.SA', 'BRFS3.SA', 'APER3.SA', 'BRKM3.SA', 'BRML3.SA', 'BRPR3.SA', 'OIBR3.SA', 'OIBR4.SA', 'B3SA3.SA', 'CARD3.SA', 'CCRO3.SA',
'CEDO4.SA', 'CEED3.SA', 'CGRA4.SA', 'CIEL3.SA', 'CMIG3.SA', 'CMIG4.SA', 'CPFE3.SA', 'CPLE3.SA', 'CRDE3.SA', 'CSAN3.SA', 'CSMG3.SA', 'CSNA3.SA', 'CTNM4.SA', 'CYRE3.SA', 'DASA3.SA',
'DIRR3.SA', 'ECOR3.SA', 'EEEL3.SA', 'ELET3.SA', 'EMBR3.SA', 'ENBR3.SA', 'ENGI4.SA', 'EQTL3.SA', 'YDUQ3.SA', 'ETER3.SA', 'EUCA4.SA', 'EVEN3.SA', 'EZTC3.SA', 'FESA4.SA', 'FHER3.SA',
'TASA4.SA', 'FLRY3.SA', 'FRIO3.SA', 'GFSA3.SA', 'GGBR3.SA', 'GGBR4.SA', 'GOAU3.SA', 'GOAU4.SA', 'GOLL4.SA', 'GRND3.SA', 'GSHP3.SA', 'HBOR3.SA', 'PRIO3.SA', 'HYPE3.SA', 'MEAL3.SA',
'INEP3.SA', 'INEP4.SA', 'ITSA3.SA', 'ITSA4.SA', 'ITUB3.SA', 'ITUB4.SA', 'JBSS3.SA', 'JFEN3.SA', 'JHSF3.SA', 'JSLG3.SA', 'KEPL3.SA', 'KLBN4.SA', 'LEVE3.SA', 'LIGT3.SA', 'LLIS3.SA',
'LOGN3.SA', 'LPSB3.SA', 'LREN3.SA', 'LUPA3.SA', 'MDIA3.SA', 'MGEL4.SA', 'MGLU3.SA', 'MILS3.SA', 'MMXM3.SA', 'ENEV3.SA', 'MRFG3.SA', 'MRVE3.SA', 'MTIG4.SA', 'MULT3.SA', 'MYPK3.SA',
'ODPV3.SA', 'OSXB3.SA', 'PDGR3.SA', 'PETR3.SA', 'PETR4.SA', 'PFRM3.SA', 'PINE4.SA', 'PLAS3.SA', 'PMAM3.SA', 'POMO3.SA', 'POMO4.SA', 'POSI3.SA', 'PSSA3.SA', 'PTBL3.SA', 'PTNT4.SA',
'ENAT3.SA', 'QUAL3.SA', 'RAPT3.SA', 'RAPT4.SA', 'RDNI3.SA', 'RENT3.SA', 'FRTA3.SA', 'ROMI3.SA', 'RSID3.SA', 'SBSP3.SA', 'SCAR3.SA', 'SGPS3.SA', 'SHOW3.SA', 'SLCE3.SA', 'SLED4.SA',
'SMTO3.SA', 'EGIE3.SA', 'TCSA3.SA', 'TECN3.SA', 'TEKA4.SA', 'TGMA3.SA', 'TOTS3.SA', 'TPIS3.SA', 'TRIS3.SA', 'TRPL4.SA', 'UGPA3.SA', 'UNIP3.SA', 'USIM3.SA', 'VALE3.SA', 'VIVR3.SA',
'VIVT3.SA', 'VLID3.SA', 'WEGE3.SA', 'SANB4.SA', 'STBP3.SA', 'CGRA3.SA', 'CLSC4.SA', 'COCE3.SA', 'TASA3.SA', 'COGN3.SA', 'RADL3.SA', 'UCAS3.SA', 'ANIM3.SA', 'BBSE3.SA', 'CVCB3.SA',
'SEER3.SA', 'TUPY3.SA', 'SQIA3.SA', 'BIOM3.SA', 'NUTR3.SA', 'TCNO3.SA', 'SANB3.SA', 'FRAS3.SA', 'GEPA4.SA', 'OFSA3.SA', 'AAPL34.SA', 'ABTT34.SA', 'AMGN34.SA', 'AMZO34.SA',
'ARMT34.SA', 'ATTB34.SA', 'AXPB34.SA', 'BERK34.SA', 'BOAC34.SA', 'CATP34.SA', 'CMCS34.SA', 'COCA34.SA', 'CSCO34.SA', 'DISB34.SA', 'EXXO34.SA', 'FDMO34.SA', 'GEOO34.SA',
'GSGI34.SA', 'HALI34.SA', 'HOME34.SA', 'HPQB34.SA', 'ITLC34.SA', 'JNJB34.SA', 'JPMC34.SA', 'LMTB34.SA', 'MMMC34.SA', 'MRCK34.SA', 'MSCD34.SA', 'MSFT34.SA', 'NIKE34.SA',
'ORCL34.SA', 'PFIZ34.SA', 'PGCO34.SA', 'SBUB34.SA', 'VERZ34.SA', 'VISA34.SA', 'WALM34.SA', 'WFCO34.SA', 'WIZS3.SA', 'BOEI34.SA', 'CHVX34.SA', 'COPH34.SA', 'CTGP34.SA',
'IBMB34.SA', 'LILY34.SA', 'MCDC34.SA', 'MSBR34.SA', 'NFLX34.SA', 'PEPB34.SA', 'SLBG34.SA', 'UPSS34.SA', 'USBC34.SA', 'GUAR3.SA', 'ACNB34.SA', 'BONY34.SA', 'DHER34.SA',
'IGBR3.SA', 'KMBB34.SA', 'METB34.SA', 'TELB4.SA', 'TGTB34.SA', 'WHRL3.SA', 'BMYB34.SA', 'COLG34.SA', 'COTY34.SA', 'EBAY34.SA', 'FCXO34.SA', 'FDXB34.SA', 'GDBR34.SA',
'HSHY34.SA', 'MDLZ34.SA', 'MOSC34.SA', 'QCOM34.SA', 'TEXA34.SA', 'WUNI34.SA', 'XRXB34.SA', 'SHUL4.SA', 'ATOM3.SA', 'KHCB34.SA', 'SAPR4.SA', 'CRIV4.SA', 'RPMG3.SA',
'BRSR3.SA', 'DMMO3.SA', 'AZEV4.SA', 'BAHI3.SA', 'BALM4.SA', 'BAUH4.SA', 'BDLL4.SA', 'BEES3.SA', 'BEES4.SA', 'BGIP4.SA', 'BMEB3.SA', 'BMEB4.SA', 'BMIN3.SA', 'BMIN4.SA',
'BMKS3.SA', 'BNBR3.SA', 'BOBR4.SA', 'BRIV3.SA', 'BRIV4.SA', 'BSLI4.SA', 'CBEE3.SA', 'CEEB3.SA', 'CGAS3.SA', 'CRIV3.SA', 'CSRN3.SA', 'CTKA4.SA', 'CTNM3.SA', 'CTSA3.SA',
'CTSA4.SA', 'DOHL4.SA', 'DTCY3.SA', 'EALT4.SA', 'EKTR4.SA', 'EMAE4.SA', 'ENGI3.SA', 'ENMT3.SA', 'ESTR4.SA', 'FESA3.SA', 'HAGA4.SA', 'HETA4.SA', 'HOOT4.SA', 'JOPA3.SA',
'JOPA4.SA', 'KLBN3.SA', 'MNDL3.SA', 'MNPR3.SA', 'MTSA4.SA', 'MWET4.SA', 'PEAB4.SA', 'PNVL3.SA', 'RANI3.SA', 'RCSL3.SA', 'RCSL4.SA', 'REDE3.SA', 'WLMM3.SA', 'WLMM4.SA',
'SLED3.SA', 'TCNO4.SA', 'TELB3.SA', 'TRPL3.SA', 'VULC3.SA', 'WHRL4.SA', 'ARNC34.SA', 'ABBV34.SA', 'BBYY34.SA', 'BIIB34.SA', 'BLAK34.SA', 'COWC34.SA', 'CTSH34.SA',
'CVSH34.SA', 'DEAI34.SA', 'FSLR34.SA', 'GILD34.SA', 'GOGL34.SA', 'GOGL35.SA', 'GPSI34.SA', 'MACY34.SA', 'MDTC34.SA', 'RIGG34.SA', 'ROST34.SA', 'SCHW34.SA',
'SSFO34.SA', 'TMOS34.SA', 'TRVC34.SA', 'USSX34.SA', 'VLOE34.SA', 'AALR3.SA', 'AFLT3.SA', 'BGIP3.SA', 'CEBR3.SA', 'CEDO3.SA', 'GPAR3.SA', 'MAPT3.SA', 'PATI4.SA',
'RPAD3.SA', 'VSPT3.SA', 'AIGB34.SA', 'DUKB34.SA', 'MOVI3.SA', 'PARD3.SA', 'AALL34.SA', 'GMCO34.SA', 'TWTR34.SA', 'UPAC34.SA', 'RNEW3.SA', 'RNEW4.SA', 'AZUL4.SA',
'TEND3.SA', 'TSLA34.SA', 'CRFB3.SA', 'IRBR3.SA', 'GEPA3.SA', 'SAPR3.SA', 'CAML3.SA', 'SUZB3.SA', 'NEOE3.SA', 'BKBR3.SA', 'FMXB34.SA', 'HONB34.SA', 'DDNB34.SA',
'UBSG34.SA', 'TXRX4.SA', 'BOXP34.SA', 'GPRO34.SA', 'MSPA3.SA', 'HAPV3.SA', 'GPIV33.SA', 'TAEE4.SA', 'ALUP3.SA', 'ALUP4.SA', 'LOGG3.SA', 'TAEE3.SA', 'AZEV3.SA',
'AHEB3.SA', 'BALM3.SA', 'BPAC3.SA', 'EALT3.SA', 'ECPR3.SA', 'HAGA3.SA', 'LIPR3.SA', 'PATI3.SA', 'PEAB3.SA', 'SULA3.SA', 'CORR4.SA', 'EEEL4.SA', 'ENMT4.SA',
'LUXM4.SA', 'MERC4.SA', 'RSUL4.SA', 'SULA4.SA', 'BBDC3.SA', 'VAMO3.SA', 'MELI34.SA', 'MOAR3.SA', 'MRSA3B.SA', 'VIVA3.SA', 'CEAB3.SA', 'BMGB4.SA'] 