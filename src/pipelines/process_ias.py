# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:50:13 2026

@author: U120137
"""

import os
import time
import pandas as pd
from src.extractors.ias_pdf import IASPdfExtractor
from src.enrichers.krs_api import KrsApiEnricher

def run_pipeline():
    # Ścieżki
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    pdf_path = os.path.join(base_dir, "data", "raw", "ias_katowice.pdf")
    output_path = os.path.join(base_dir, "data", "processed", "enriched_crypto_register.csv")

    print(f"1. Rozpoczęto analizę PDF: {pdf_path}")
    extractor = IASPdfExtractor(pdf_path)
    df_raw = extractor.extract_table()
    df_cleaned = extractor.clean_krs_column(df_raw)
    
    print(f"Wczytano {len(df_cleaned)} podmiotów.")
    
    enricher = KrsApiEnricher()
    enriched_results =[]

    print("2. Rozpoczęto pobieranie danych z KRS (to może potrwać)...")
    for index, row in df_cleaned.iterrows():
        krs = row.get('clean_krs')
        nazwa = row.get(df_cleaned.columns[1], 'Nieznany') # Zakładam 2 kolumna to nazwa
        
        if pd.notna(krs):
            # API KRS nakłada limity, warto zrobić przerwę (rate-limiting)
            time.sleep(0.5) 
            
            raw_krs_data = enricher.fetch_entity_data(krs)
            parsed_data = enricher.parse_krs_json(raw_krs_data)
            
            row_dict = row.to_dict()
            row_dict.update(parsed_data)
            enriched_results.append(row_dict)
            print(f"[OK] Pobrano dane dla KRS: {krs} ({nazwa})")
        else:
            row_dict = row.to_dict()
            row_dict.update({
                "krs_status": "Brak KRS", "likwidacja": "", "krs_adres": "", 
                "osoby_decyzyjne": "", "udzialowcy": ""
            })
            enriched_results.append(row_dict)

    print("3. Zapisywanie wyników...")
    df_final = pd.DataFrame(enriched_results)
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Sukces! Plik zapisany w: {output_path}")

if __name__ == "__main__":
    run_pipeline()