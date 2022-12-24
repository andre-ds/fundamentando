import os
from pyspark.sql.types import StructField, StructType, DateType, DoubleType, StringType, IntegerType, FloatType


PATH_DATALAKE = '/datalake'
DIR_PATH_RAW_DFP = os.path.join(PATH_DATALAKE, 'raw-dfp')
DIR_PATH_RAW_ITR = os.path.join(PATH_DATALAKE, 'raw-itr')
DIR_PATH_RAW_STOCK = os.path.join(PATH_DATALAKE, 'raw-stock')
DIR_PATH_PROCESSED_DFP = os.path.join(PATH_DATALAKE, 'pre-processed-dfp')
DIR_PATH_PROCESSED_ITR = os.path.join(PATH_DATALAKE, 'pre-processed-itr')
DIR_PATH_PROCESSED_STOCK = os.path.join(PATH_DATALAKE, 'pre-processed-stock')
DIR_PATH_ANALYTICAL = os.path.join(PATH_DATALAKE, 'analytical')


DIR_S3_RAW_DFP = 's3://fundamentus-pre-processed-dfp'
DIR_S3_RAW_ITR = 's3://fundamentus-pre-processed-itr'
DIR_S3_ANALYTICAL = 's3://fundamentus-analytical'
DIR_S3_STOCKS = 's3://fundamentus-stock-raw'
DIR_S3_PROCESSED_STOCKS = 's3://fundamentus-pre-processed-stock'

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

'''
schema_ticker = StructType([
    StructField('variable_type', StringType(), True),
    StructField('ticker', StringType(), True),
    StructField('value', FloatType(), True)
])
'''

schema_ticker = StructType([
    StructField('ticker', StringType(), True),
    StructField('date', StringType(), True),
    StructField('open', FloatType(), True),
    StructField('high', FloatType(), True),
    StructField('low', FloatType(), True),
    StructField('close', FloatType(), True),
    StructField('adj_close', FloatType(), True),
    StructField('volume', IntegerType(), True),
    StructField('dividends', IntegerType(), True),
    StructField('stock_splits', IntegerType(), True)
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

tickers = ['ABCB4.SA', 'AGRO3.SA', 'RAIL3.SA', 'ALPA3.SA', 'ALPA4.SA', 'ALSO3.SA', 'AMAR3.SA', 'ABEV3.SA', 'ADHM3.SA', 'ARZZ3.SA',
    'BBAS3.SA', 'BBDC3.SA', 'BBDC4.SA', 'BBRK3.SA', 'BEEF3.SA', 'BPHA3.SA', 'BPAN4.SA', 'BRAP3.SA', 'BRAP4.SA', 'BRFS3.SA', 'APER3.SA', 'BRKM3.SA',
    'BRKM5.SA', 'BRML3.SA', 'BRPR3.SA', 'BRSR6.SA', 'OIBR3.SA', 'OIBR4.SA', 'BTOW3.SA', 'B3SA3.SA', 'CAMB4.SA', 'CARD3.SA', 'CCPR3.SA', 'CCRO3.SA',
    'CEDO4.SA', 'CEED3.SA', 'CESP6.SA', 'CGAS5.SA', 'CGRA4.SA', 'CIEL3.SA', 'CMIG3.SA', 'CMIG4.SA', 'COCE5.SA', 'CPFE3.SA', 'CPLE3.SA', 'CPLE6.SA',
    'CRDE3.SA', 'CSAN3.SA', 'CSMG3.SA', 'CSNA3.SA', 'LIQO3.SA', 'CTNM4.SA', 'CYRE3.SA', 'DASA3.SA', 'DIRR3.SA', 'DTEX3.SA', 'ECOR3.SA', 'EEEL3.SA',
    'ELEK4.SA', 'ELET3.SA', 'ELET6.SA', 'EMBR3.SA', 'ENBR3.SA', 'ENGI4.SA', 'EQTL3.SA', 'YDUQ3.SA', 'ETER3.SA', 'EUCA4.SA', 'EVEN3.SA', 'EZTC3.SA',
    'FESA4.SA', 'FHER3.SA', 'TASA4.SA', 'FLRY3.SA', 'FRIO3.SA', 'TIET3.SA', 'TIET4.SA', 'GFSA3.SA', 'GGBR3.SA', 'GGBR4.SA', 'GOAU3.SA', 'GOAU4.SA',
    'GOLL4.SA', 'GRND3.SA', 'GSHP3.SA', 'HBOR3.SA', 'HGTX3.SA', 'PRIO3.SA', 'HYPE3.SA', 'IDNT3.SA', 'IDVL4.SA', 'IGTA3.SA', 'MEAL3.SA', 'INEP3.SA',
    'INEP4.SA', 'ITSA3.SA', 'ITSA4.SA', 'ITUB3.SA', 'ITUB4.SA', 'JBSS3.SA', 'JFEN3.SA', 'JHSF3.SA', 'JSLG3.SA', 'KEPL3.SA', 'KLBN4.SA', 'LAME3.SA',
    'LAME4.SA', 'LEVE3.SA', 'LIGT3.SA', 'LLIS3.SA', 'LOGN3.SA', 'LPSB3.SA', 'LREN3.SA', 'LUPA3.SA', 'MDIA3.SA', 'MGEL4.SA', 'MGLU3.SA', 'MILS3.SA',
    'MMXM3.SA', 'ENEV3.SA', 'MRFG3.SA', 'MRVE3.SA', 'MTIG4.SA', 'MULT3.SA', 'MYPK3.SA', 'NATU3.SA', 'ODPV3.SA', 'OSXB3.SA', 'PCAR4.SA', 'PDGR3.SA',
    'PETR3.SA', 'PETR4.SA', 'PFRM3.SA', 'PINE4.SA', 'PLAS3.SA', 'PMAM3.SA', 'POMO3.SA', 'POMO4.SA', 'POSI3.SA', 'PSSA3.SA', 'PTBL3.SA', 'PTNT4.SA',
    'ENAT3.SA', 'QUAL3.SA', 'RAPT3.SA', 'RAPT4.SA', 'RDNI3.SA', 'RENT3.SA', 'FRTA3.SA', 'RNEW11.SA', 'ROMI3.SA', 'RSID3.SA', 'SANB11.SA', 'SBSP3.SA',
    'SCAR3.SA', 'SGPS3.SA', 'SHOW3.SA', 'SLCE3.SA', 'SLED4.SA', 'SMTO3.SA', 'SULA11.SA', 'EGIE3.SA', 'TCSA3.SA', 'TECN3.SA', 'TEKA4.SA', 'TGMA3.SA',
    'TIMP3.SA', 'TOTS3.SA', 'TPIS3.SA', 'TRIS3.SA', 'TAEE11.SA', 'TRPL4.SA', 'TRPN3.SA', 'UGPA3.SA', 'UNIP3.SA', 'UNIP6.SA', 'USIM3.SA', 'USIM5.SA',
    'TESA3.SA', 'VALE3.SA', 'VIVR3.SA', 'VIVT3.SA', 'VIVT4.SA', 'VLID3.SA', 'WEGE3.SA', 'BRSR5.SA', 'CESP5.SA', 'ELPL3.SA', 'SANB4.SA', 'STBP3.SA',
    'CCXC3.SA', 'CESP3.SA', 'CGRA3.SA', 'CLSC4.SA', 'COCE3.SA', 'TASA3.SA', 'COGN3.SA', 'LCAM3.SA', 'RADL3.SA', 'UCAS3.SA', 'ALUP11.SA', 'ANIM3.SA',
    'BBSE3.SA', 'BSEV3.SA', 'CPRE3.SA', 'CVCB3.SA', 'LINX3.SA', 'SEER3.SA', 'SMLS3.SA', 'TUPY3.SA', 'SQIA3.SA', 'BIOM3.SA', 'KLBN11.SA', 'NUTR3.SA',
    'BAZA3.SA', 'HGBS11.SA', 'HGPO11.SA', 'MBRF11.SA', 'TCNO3.SA', 'ABCP11.SA', 'RBED11.SA', 'BBPO11.SA', 'BBRC11.SA', 'BBVJ11.SA', 'BPFF11.SA',
    'BRCR11.SA', 'CTXT11.SA', 'CXTL11.SA', 'EURO11.SA', 'RCRB11.SA', 'FIGS11.SA', 'FIXX11.SA', 'FLMA11.SA', 'FPAB11.SA', 'HMOC11.SA', 'HGCR11.SA',
    'HGLG11.SA', 'HGRE11.SA', 'JSRE11.SA', 'KNCR11.SA', 'KNRE11.SA', 'KNRI11.SA', 'MXRF11.SA', 'ONEF11.SA', 'PQDP11.SA', 'PRSV11.SA', 'RBBV11.SA',
    'RBCB11.SA', 'RBGS11.SA', 'RBRD11.SA', 'RBVO11.SA', 'RDES11.SA', 'RNDP11.SA', 'RNGO11.SA', 'SAAG11.SA', 'SDIL11.SA', 'SHPH11.SA', 'SPTW11.SA',
    'TBOF11.SA', 'TRXL11.SA', 'VLOL11.SA', 'VRTA11.SA', 'XPCM11.SA', 'XTED11.SA', 'SANB3.SA', 'SEDU3.SA', 'FRAS3.SA', 'GEPA4.SA', 'OFSA3.SA', 'RLOG3.SA',
    'TOYB3.SA', 'AAPL34.SA', 'ABTT34.SA', 'AMGN34.SA', 'AMZO34.SA', 'ARMT34.SA', 'ATTB34.SA', 'AXPB34.SA', 'BERK34.SA', 'BOAC34.SA', 'CATP34.SA',
    'CMCS34.SA', 'COCA34.SA', 'CSCO34.SA', 'DISB34.SA', 'EXXO34.SA', 'FDMO34.SA', 'GEOO34.SA', 'GSGI34.SA', 'HALI34.SA', 'HOME34.SA', 'HPQB34.SA',
    'ITLC34.SA', 'JNJB34.SA', 'JPMC34.SA', 'LMTB34.SA', 'MMMC34.SA', 'MRCK34.SA', 'MSCD34.SA', 'MSFT34.SA', 'NIKE34.SA', 'ORCL34.SA', 'PFIZ34.SA',
    'PGCO34.SA', 'SBUB34.SA', 'VERZ34.SA', 'VISA34.SA', 'WALM34.SA', 'WFCO34.SA', 'WIZS3.SA', 'BOEI34.SA', 'CHVX34.SA', 'COPH34.SA', 'CTGP34.SA',
    'FBOK34.SA', 'IBMB34.SA', 'LILY34.SA', 'MCDC34.SA', 'MFII11.SA', 'MSBR34.SA', 'NFLX34.SA', 'PEPB34.SA', 'SLBG34.SA', 'UPSS34.SA', 'USBC34.SA',
    'GUAR3.SA', 'ACNB34.SA', 'BONY34.SA', 'DHER34.SA', 'IGBR3.SA', 'KMBB34.SA', 'METB34.SA', 'TELB4.SA', 'TGTB34.SA', 'WHRL3.SA', 'AVON34.SA',
    'BMYB34.SA', 'COLG34.SA', 'COTY34.SA', 'EBAY34.SA', 'FCXO34.SA', 'FDXB34.SA', 'GDBR34.SA', 'HSHY34.SA', 'MDLZ34.SA', 'MOSC34.SA', 'QCOM34.SA',
    'TEXA34.SA', 'TIFF34.SA', 'UTEC34.SA', 'WUNI34.SA', 'XRXB34.SA', 'SHUL4.SA', 'ATOM3.SA', 'KHCB34.SA', 'TIET11.SA', 'SAPR4.SA', 'CRIV4.SA',
    'RPMG3.SA', 'BRSR3.SA', 'DOMC11.SA', 'ENGI11.SA', 'SNSY5.SA', 'DMMO3.SA', 'ABCB10.SA', 'ANIM3T.SA', 'AZEV4.SA', 'BAHI3.SA', 'BALM4.SA',
    'BAUH4.SA', 'BDLL4.SA', 'BEES3.SA', 'BEES4.SA', 'BGIP4.SA', 'BMEB3.SA', 'BMEB4.SA', 'BMIN3.SA', 'BMIN4.SA', 'BMKS3.SA', 'BNBR3.SA',
    'BOBR4.SA', 'BRIV3.SA', 'BRIV4.SA', 'BSLI4.SA', 'BTTL3.SA', 'CBEE3.SA', 'CEEB3.SA', 'CEEB5.SA', 'CELP3.SA', 'CELP5.SA', 'CEPE5.SA',
    'CGAS3.SA', 'CPRE3T.SA', 'CRIV3.SA', 'CRPG5.SA', 'CRPG6.SA', 'CSRN3.SA', 'CTKA4.SA', 'CTNM3.SA', 'CTSA3.SA', 'CTSA4.SA', 'CVCB3T.SA',
    'DOHL4.SA', 'DTCY3.SA', 'EALT4.SA', 'EKTR4.SA', 'ELEK3.SA', 'EMAE4.SA', 'ENGI3.SA', 'ENMT3.SA', 'ESTR4.SA', 'FESA3.SA', 'FNAM11.SA',
    'FSRF11.SA', 'GPCP3.SA', 'HAGA4.SA', 'HBTS5.SA', 'HETA4.SA', 'HOOT4.SA', 'IDVL3.SA', 'JBDU3.SA', 'JBDU4.SA', 'JOPA3.SA', 'JOPA4.SA',
    'KLBN3.SA', 'MEND5.SA', 'MNDL3.SA', 'MNPR3.SA', 'MTSA4.SA', 'MWET4.SA', 'DMMO3T.SA', 'PEAB4.SA', 'PNVL3.SA', 'PNVL4.SA', 'RANI3.SA',
    'RANI4.SA', 'RCSL3.SA', 'RCSL4.SA', 'REDE3.SA', 'RPAD5.SA', 'RPAD6.SA', 'SEER3T.SA', 'WLMM3.SA', 'WLMM4.SA', 'SLED3.SA', 'SMLS3T.SA',
    'SOND5.SA', 'SOND6.SA', 'SPRI3.SA', 'TCNO4.SA', 'TELB3.SA', 'TOYB4.SA', 'TRPL3.SA', 'UNIP5.SA', 'USIM6.SA', 'VULC3.SA', 'VVAR3.SA',
    'WHRL4.SA', 'ALMI11.SA', 'ANCR11B.SA', 'ATSA11B.SA', 'BBFI11B.SA', 'BCFF11.SA', 'BMLC11B.SA', 'CEOC11.SA', 'CNES11.SA', 'MRSA6BF.SA',
    'CPTS11B.SA', 'CXCE11B.SA', 'DRIT11B.SA', 'EDFO11B.SA', 'EDGA11.SA', 'ENMA3B.SA', 'FAED11.SA', 'FAMB11B.SA', 'FCFL11.SA', 'FEXC11.SA',
    'FIIP11B.SA', 'FLRP11.SA', 'FVBI11.SA', 'HCRI11.SA', 'HTMX11.SA', 'JRDM11.SA', 'MAXR11.SA', 'NSLU11.SA', 'TRNT11.SA', 'WPLZ11B.SA', 'ARNC34.SA',
    'ABBV34.SA', 'BBYY34.SA', 'BCRI11.SA', 'BIIB34.SA', 'BLAK34.SA', 'CHKE34.SA', 'COWC34.SA', 'CTSH34.SA', 'CVSH34.SA', 'CXRI11.SA', 'DEAI34.SA',
    'FSLR34.SA', 'GILD34.SA', 'GOGL34.SA', 'GOGL35.SA', 'GPSI34.SA', 'JCPC34.SA', 'MACY34.SA', 'MDTC34.SA', 'RIGG34.SA', 'ROST34.SA', 'SANC34.SA',
    'SCHW34.SA', 'SPRN34.SA', 'SSFO34.SA', 'TMOS34.SA', 'TRVC34.SA', 'USSX34.SA', 'VLOE34.SA', 'WSON33.SA', 'AALR3.SA', 'AFLT3.SA', 'BGIP3.SA',
    'BNFS11.SA', 'CBOP11.SA', 'CEBR3.SA', 'CEDO3.SA', 'ESUD11.SA', 'ESUT11.SA', 'FIIB11.SA', 'FIVN11.SA', 'FMOF11.SA', 'FNOR11.SA', 'FSPE11.SA',
    'FSTU11.SA', 'GPAR3.SA', 'GRLV11.SA', 'MAPT3.SA', 'MMXM11.SA', 'PABY11.SA', 'PATI4.SA', 'PLRI11.SA', 'PORD11.SA', 'RBDS11.SA', 'RPAD3.SA',
    'SCPF11.SA', 'VSPT3.SA', 'XPOM11.SA', 'POMO10.SA', 'AIGB34.SA', 'DUKB34.SA', 'MOVI3.SA', 'PARD3.SA', 'BPAC11.SA', 'PPLA11.SA', 'AALL34.SA',
    'GMCO34.SA', 'LBRN34.SA', 'TWTR34.SA', 'UPAC34.SA', 'RNEW3.SA', 'RNEW4.SA', 'AZUL4.SA', 'KNIP11.SA', 'TEND3.SA', 'TSLA34.SA', 'CRFB3.SA',
    'GBIO33.SA', 'OMGE3.SA', 'IRBR3.SA', 'GGRC11.SA', 'GEPA3.SA', 'SAPR3.SA', 'CAML3.SA', 'VISC11.SA', 'SUZB3.SA', 'SAPR11.SA', 'BRDT3.SA', 'NEOE3.SA',
    'BKBR3.SA', 'FMXB34.SA', 'CLGN34.SA', 'HONB34.SA', 'ALZR11.SA', 'GFSA1.SA', 'DDNB34.SA', 'UBSG34.SA', 'TXRX4.SA', 'OUJP11.SA', 'BCIA11.SA', 'BOXP34.SA',
    'GPRO34.SA', 'MSPA3.SA', 'HFOF11.SA', 'RBRF11.SA', 'HAPV3.SA', 'GNDI3.SA', 'BIDI4.SA', 'JPSA3.SA', 'GPIV33.SA', 'IRDM11.SA', 'FOFT11.SA', 'THRA11.SA',
    'TAEE4.SA', 'BIDI4T.SA', 'XPLG11.SA', 'RBRR11.SA', 'WPLZ11.SA', 'MGFF11.SA', 'TGAR11.SA', 'HGRU11.SA', 'ALUP3.SA', 'ALUP4.SA', 'LOGG3.SA', 'KNHY11.SA',
    'OUCY11.SA', 'FVPQ11.SA', 'TASA13.SA', 'TASA15.SA', 'TASA17.SA', 'CARE11.SA', 'TAEE3.SA', 'XPIN11.SA', 'VSHO11.SA', 'DMMO11.SA', 'AZEV3.SA', 'AHEB3.SA',
    'BALM3.SA', 'BPAC3.SA', 'EALT3.SA', 'ECPR3.SA', 'HAGA3.SA', 'ITEC3.SA', 'LIPR3.SA', 'PATI3.SA', 'PEAB3.SA', 'SULA3.SA', 'BPAC5.SA', 'BRGE11.SA',
    'BRGE12.SA', 'BRGE6.SA', 'CELP6.SA', 'CELP7.SA', 'CORR4.SA', 'EEEL4.SA', 'ENMT4.SA', 'LUXM4.SA', 'MEND6.SA', 'MERC4.SA', 'RSUL4.SA', 'SULA4.SA',
    'BEEF11.SA', 'JBDU1.SA', 'JBDU2.SA', 'LOGN12.SA', 'MYPK12.SA', 'PATC11.SA', 'BBDC3.SA', 'VGIR11.SA', 'VILG11.SA', 'CNTO3.SA', 'BTCR11.SA', 'VAMO3.SA',
    'CAMB10.SA', 'IDVL9.SA', 'MELI34.SA', 'MOAR3.SA', 'RBVA11.SA', 'MRSA3B.SA', 'VTLT11.SA', 'GTWR11.SA', 'BIDI11.SA', 'BIDI3.SA', 'DMAC11.SA', 'TFOF11.SA',
    'HCTR11.SA', 'TIET2.SA', 'HSML11.SA', 'XPHT11.SA', 'XPHT12.SA', 'HABT11.SA', 'RECT11.SA', 'AQLL11.SA', 'ARFI11B.SA', 'ATCR11.SA', 'BARI11.SA', 'BBIM11.SA',
    'BMII11.SA', 'BPRP11.SA', 'BRHT11B.SA', 'BVAR11.SA', 'DAMT11B.SA', 'DOVL11B.SA', 'ELDO11B.SA', 'ERPA11.SA', 'FINF11.SA', 'FISC11.SA', 'FISD11.SA',
    'FPNG11.SA', 'FTCE11B.SA', 'GESE11B.SA', 'HBTT11.SA', 'HGFF11.SA', 'HUSC11.SA', 'JPPC11.SA', 'KINP11.SA', 'LATR11B.SA', 'NCHB11.SA', 'NPAR11.SA',
    'NVHO11.SA', 'NVIF11B.SA', 'ORPD11.SA', 'OULG11B.SA', 'PBLV11.SA', 'PRSN11B.SA', 'PRTS11.SA', 'RBCO11.SA', 'RBFF11.SA', 'RBRP11.SA', 'RBRY11.SA',
    'RCFA11.SA', 'RCRI11B.SA', 'RDPD11.SA', 'REIT11.SA', 'SADI11.SA', 'SAIC11B.SA', 'SFND11.SA', 'SHDP11B.SA', 'SHOP11.SA', 'SPAF11.SA', 'STRX11.SA',
    'TCR11.SA', 'TORM13.SA', 'TOUR11.SA', 'TSNC11.SA', 'VERE11.SA', 'VLJS11.SA', 'VPSI11.SA', 'WTSP11B.SA', 'YCHY11.SA', 'VIVA3.SA', 'CEAB3.SA',
    'BMGB11.SA', 'BMGB4.SA', 'SDIP11.SA', 'RBCO11.SA', 'IBFF11.SA', 'CVBI11.SA', 'HRDF11.SA', 'RSPD11.SA', 'TCPF11.SA']
