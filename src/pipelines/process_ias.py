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
import time
from datetime import timedelta

def run_pipeline():
    # Ścieżki
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
    pdf_path = os.path.join(base_dir, "data", "raw", "ias_katowice.pdf")
    output_path = os.path.join(base_dir, "data", "processed", "enriched_crypto_register.csv")
    start_time_pdf = time.perf_counter()
    print(f"1. Rozpoczęto analizę PDF: {pdf_path}", flush=True)
    extractor = IASPdfExtractor(pdf_path)
    df_raw = extractor.extract_table()
    df_cleaned = extractor.clean_krs_column(df_raw)
    
    pdf_duration = str(timedelta(seconds=int(time.perf_counter() - start_time_pdf)))
    print(f"Wczytano {len(df_cleaned)} podmiotów. Czas pracy: {pdf_duration}", flush=True)
    
    
    
    
    
    enricher = KrsApiEnricher()
    enriched_results =[]
    
    print("2. Rozpoczęto pobieranie danych z KRS (to może potrwać)...", flush=True)
    start_time_global = time.perf_counter()
    total = len(df_cleaned)
    
    for index, row in df_cleaned.iterrows():
        start_time_row = time.perf_counter()
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
            
            end_time_row = time.perf_counter()
            duration_row = end_time_row - start_time_row
    
            elapsed_total = end_time_row - start_time_global
            avg_time_per_row = elapsed_total / (index+1)
            remaining_rows = total - (index+1)
            eta_seconds = remaining_rows * avg_time_per_row
    
            # Formatowanie czasów do czytelnej postaci HH:MM:SS
            
            eta_str = str(timedelta(seconds=int(eta_seconds)))
            
            print(f"[OK] Pobrano dane dla KRS: {krs} ({nazwa}), czas wiersza {duration_row:5.2f}s, ETA: {eta_str}", flush=True)
        else:
            row_dict = row.to_dict()
            row_dict.update({
                "krs_status": "Brak KRS", "likwidacja": "", "krs_adres": "", 
                "osoby_decyzyjne": "", "udzialowcy": ""
            })
            enriched_results.append(row_dict)

    print("3. Zapisywanie wyników...", flush=True)
    df_final = pd.DataFrame(enriched_results)
    df_final.to_csv(output_path, index=False, encoding='utf-8')
    total_duration = str(timedelta(seconds=int(time.perf_counter() - start_time_global)))
    print(f"Sukces! Plik zapisany w: {output_path}. Całkowity czas pracy: {total_duration}")
    
    
if __name__ == "__main__":
    run_pipeline()
