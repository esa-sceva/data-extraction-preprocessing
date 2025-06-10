from presidio_helpers import analyzer_engine, analyze

analyzer_params = ("flair", "flair/ner-english-large", "", "")

# st_text = "My name is John Doe and my phone number is 212-555-1234. Email me at john.doe@example.com."

st_text = """
* Aramburu Merlos and Hijmans (2020)Aramburu Merlos, F., & Hijmans, R. J. (2020). The scale dependency of spatial crop species diversity and its relation to temporal diversity. _Proceedings of the National Academy of Sciences_, _117_, 26176-26182.
* Beck et al. (2018) Beck, H. E., Zimmermann, N. E., McVicar, T. R., Vergopolan, N., Berg, A., & Wood, E. F. (2018). Present and future koppen-geiger climate classification maps at 1-km resolution. _Scientific data_, \\(5\\), 1-12.
* Bohan et al. (2021) Bohan, D. A., Schmucki, R., Abay, A. T., Termansen, M., Bane, M., Charalabidis, A., Cong, R.-G., Derocles, S. A., Dorner, Z., Forster, M. et al. (2021). Designing farmer-acceptable rotations that assure ecosystem service provision in the face of climate change. In _Advances in ecological research_ (pp. 169-244). Elsevier volume 65.
* Bonneuil et al. (2012) Bonneuil, C., Goffaux, R., Bonnin, I., Montalent, P., Hamon, C., Balfourier, F., & Goldringer, I. (2012). A new integrative indicator to assess crop genetic diversity. _Ecological Indicators_, _23_, 280-289. URL: [https://www.sciencedirect.com/science/article/pii/S1470160X12001641](https://www.sciencedirect.com/science/article/pii/S1470160X12001641). doi:[https://doi.org/10.1016/j.ecolind.2012.04.002](https://doi.org/10.1016/j.ecolind.2012.04.002).
* Davis et al. (2012) Davis, A. S., Hill, J. D., Chase, C. A., Johanns, A. M., & Liebman, M. (2012). Increasing cropping system diversity balances productivity, profitability and environmental health,.
* Di Falco et al. (2010) Di Falco, S., Penov, I., Aleksiev, A., & Van Rensburg, T. M. (2010). Agrobiodiversity, farm profits and land fragmentation: Evidence from bulgaria. _Land use policy_, _27_, 763-771.
* Donfouet et al. (2017) Donfouet, H. P. P., Barczak, A., Detang-Dessendre, C., & Maigne, E. (2017). Crop production and crop diversity in france: a spatial analysis. _Ecological Economics_, _134_, 29-39.
* D'Amico et al. (2017)* d'Andrimont et al. (2021) d'Andrimont, R., Verhegghen, A., Lemoine, G., Kempeneers, P., Meroni, M., & Van der Velde, M. (2021). From parcel to continental scale-a first european crop type map based on sentinel-1 and lucas copernicus in-situ observations. _Remote sensing of environment, 266_, 112708.
* Egli et al. (2021) Egli, L., Schroter, M., Scherber, C., Tschrantke, T., & Seppelt, R. (2021). Crop diversity effects on temporal agricultural production stability across european regions. _Regional Environmental Change_, _21_, 1-12.
* Elmqvist et al. (2003) Elmqvist, T., Folke, C., Nystrom, M., Peterson, G., Bengtsson, J., Walker, B., & Norberg, J. (2003). Response diversity, ecosystem change, and resilience. _Frontiers in Ecology and the Environment_, \\(1\\), 488-494.
* European Commission () European Commission (). Common monitoring and evaluation framework. [https://agriculture.ec.europa.eu/common-agricultural-policy/cap-overview/cmeef_en](https://agriculture.ec.europa.eu/common-agricultural-policy/cap-overview/cmeef_en). Accessed: 2023-08-01.
* Gil et al. (2017) Gil, J. D., Cohn, A. S., Duncan, J., Newton, P., & Vermeulen, S. (2017). The resilience of integrated agricultural systems to climate change. _Wiley Interdisciplinary Reviews: Climate Change_, \\(8\\), e461.
* Hajjar et al. (2008) Hajjar, R., Jarvis, D. I., & Gemmill-Herren, B. (2008). The utility of crop genetic diversity in maintaining ecosystem services. _Agriculture, Ecosystems & Environment_, _123_, 261-270.
* Jost (2006) Jost, L. (2006). Entropy and diversity. _Oikos_, _113_, 363-375.
* Jost (2007) Jost, L. (2007). Partitioning diversity into independent alpha and beta components. _Ecology_, _88_, 2427-2439.
* Kahluoto et al. (2019) Kahluoto, H., Kaseva, J., Balek, J., Olesen, J. E., Ruiz-Ramos, M., Gobin, A., Kersebaum, K. C., Takac, J., Ruget, F., Ferrise, R. et al. (2019). Decline in climate resilience of european wheat. _Proceedings of the National Academy of Sciences_, _116_, 123-128.
* Krishnaswamy et al. (2009) Krishnaswamy, J., Bawa, K. S., Ganeshaiah, K., & Kiran, M. (2009). Quantifying and mapping biodiversity and ecosystem services: Utility of a multi-season ndvi based mahalanobis distance surrogate. _Remote Sensing of Environment_, _113_, 857-867.
* Lazikova et al. (2019) Lazikova, J., Bandlerova, A., Rumanovska, L., Takac, I., & Lazikova, Z. (2019). Crop diversity and common agricultural policy--the case of Slovakia. _Sustainability_, _11_, 1416.
* MacArthur (1965) MacArthur, R. H. (1965). Patterns of species diversity. _Biological reviews_, _40_, 510-533.
* Machefer et al. (2022) Machefer, d. R., Melissande, Zampieri, M., Claverie, M., Dentener, F., & Van Der Velde, M. (2022). Eu crop diversity,. URL: [http://data.europa.eu/89h/f2c60a95-d7ddd-44c9-9b86-6eb27871907](http://data.europa.eu/89h/f2c60a95-d7ddd-44c9-9b86-6eb27871907).
* Mahy et al. (2015) Mahy, L., Dupeux, B. E. T. I., Van Huylenbroeck, G., & Buysse, J. (2015). Simulating farm level response to crop diversification policy. _Land Use Policy_, _45_, 36-42.
* Margalef (1973) Margalef, R. (1973). Information theory in ecology,.
* Martin & Magne (2015) Martin, G., & Magne, M.-A. (2015). Agricultural diversity to increase adaptive capacity and reduce vulnerability of livestock systems against weather variability-a farm-scale simulation study. _Agriculture, Ecosystems & Environment_, _199_, 301-311.
* Prober & Smith (2009) Prober, S. M., & Smith, F. P. (2009). Enhancing biodiversity persistence in intensively used agricultural landscapes: a synthesis of 30 years of research in the western australian wheatbelt. _Agriculture, ecosystems & environment_, _132_, 173-191.
* Raderschall et al. (2021) Raderschall, C. A., Bommarco, R., Lindstrom, S. A., & Lundin, O. (2021). Landscape crop diversity and semi-natural habitat affect crop pollinators, pollination benefit and yield. _Agriculture, Ecosystems & Environment_, _306_, 107189.
* Renard & Tilman (2019) Renard, D., & Tilman, D. (2019). National food production stabilized by crop diversity. _Nature_, _571_, 257-260.
* Schaak et al. (2023) Schaak, H., Bommarco, R., Hansson, H., Kuns, B., & Nilsson, P. (2023). Long-term trends in functional crop diversity across swedish farms. _Agriculture, Ecosystems & Environment_, _343_, 108269.
* Shannon (1948) Shannon, C. E. (1948). A mathematical theory of communication. _The Bell system technical journal_, _27_, 379-423.
* Simpson (1949) Simpson, E. H. (1949). Measurement of diversity. _nature_, _163_, 688-688.
* Smith et al. (2008) Smith, R. G., Gross, K. L., & Robertson, G. P. (2008). Effects of crop diversity on agroecosystem function: crop yield response. _Ecosystems_, _11_, 355-366.
* Soille et al. (2018) Soille, P., Burger, A., De Marchi, D., Kempeneers, P., Rodriguez, D., Syrris, V., & Vasilev, V. (2018). A versatile data-intensivecomputing platform for information retrieval from big geospatial data. Future Generation Computer Systems8130-40. Cited by: SS1.
* C. D. Sprunger, T. Martin, and M. Mann (2020)Systems with greater perenniality and crop diversity enhance soil biological health. Agricultural & Environmental Letters5 (e), e20030. Cited by: SS1.
* E. Thomine, J. Mumford, A. Rusch, and N. Desneux (2022)Using crop diversity to lower pesticide use: socio-ecological approaches. Science of the Total Environment804, pp. 150156. Cited by: SS1.
* H. Tuomisto (2010)A diversity of beta diversities: straightening up a concept gone awry. part 1. defining beta diversity as a function of alpha and gamma diversity. Ecography33, pp. 2-22. Cited by: SS1.
* S. Uthes, E. Kelly, and H. J. Konig (2020)Farm-level indicators for crop and landscape diversity derived from agricultural beneficiaries data. Ecological Indicators108, pp. 105725. Cited by: SS1.
* R. Weigel, T. Koellner, P. Poppenborg, and C. Bogner (2018)Crop diversity and stability of revenue on farms in central europe: an analysis of big data from a comprehensive agricultural census in Bavaria. PLoS One13, pp. e0207454. Cited by: SS1.
* S. A. Wood, D. S. Karp, F. DeClerck, C. Kremen, S. Naeem, and C. A. Palm (2015)Functional traits in agriculture: agrobiodiversity and ecosystem services. Trends in ecology & evolution30, pp. 531-539. Cited by: SS1.
* M. Zampieri, A. Toreti, A. Ceglar, G. Naumann, M. Turco, and C. Tebaldi (2020)Climate resilience of the top ten wheat producers in the mediterranean and the middle east. Regional Environmental Change20, pp. 1-9. Cited by: SS1.
* M. Zampieri, C. J. Weissteiner, B. Grizzetti, A. Toreti, and F. van den Berg (2020)Estimating resilience of crop production systems: from theory to practice. Science of the Total Environment735, pp. 139378. Cited by: SS1.
"""

analyzer = analyzer_engine(*analyzer_params)

st_analyze_results = analyze(
    *analyzer_params,
    text=st_text,
    entities=None,  # Use all supported entities
    language="en",
    score_threshold=0.35,
    return_decision_process=False,
    allow_list=[],
    deny_list=[]
)

for res in st_analyze_results:
    entity_text = st_text[res.start:res.end]
    print(f"Entity type: {res.entity_type}, Text: {entity_text}, Start: {res.start}, End: {res.end}, Confidence: {res.score}")


def anonymize_text(text, results):
    results_sorted = sorted(results, key=lambda r: r.start, reverse=True)
    for res in results_sorted:
        placeholder = f"[{res.entity_type.upper()}]"
        text = text[:res.start] + placeholder + text[res.end:]
    return text

anonymized_text = anonymize_text(st_text, st_analyze_results)
print(anonymized_text)