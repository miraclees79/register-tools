# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:49:31 2026

@author: U120137
"""

import pdfplumber
import pandas as pd
import re

class IASPdfExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path

    def extract_table(self) -> pd.DataFrame:
        all_rows =[]
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                # extract_table zwraca listę list (wiersze i kolumny)
                table = page.extract_table()
                if table:
                    all_rows.extend(table)
        
        if not all_rows:
            raise ValueError("Nie znaleziono tabeli w pliku PDF.")

        # Pierwszy wiersz zazwyczaj zawiera nagłówki (np. "L.p.", "Nazwa", "NIP", "KRS")
        # Należy oczyścić nagłówki z białych znaków (np. \n)
        headers = [str(h).replace('\n', ' ').strip() if h else f"Col_{i}" for i, h in enumerate(all_rows[0])]
        
        df = pd.DataFrame(all_rows[1:], columns=headers)
        
        # Czyszczenie: usunięcie pustych wierszy
        df.dropna(how='all', inplace=True)
        
        return df

    def clean_krs_column(self, df: pd.DataFrame, krs_col_name: str = 'Numer KRS') -> pd.DataFrame:
        """
        Próbuje zlokalizować kolumnę z KRS, wyciąga 10-cyfrowy numer, 
        filtruje tylko te podmioty, które faktycznie go posiadają (np. spółki z o.o., S.A.).
        Osoby fizyczne prowadzące działalność (CEIDG) zostaną pominięte w kontekście API KRS.
        """
        # Znajdź kolumnę, która w nazwie ma "KRS" (wielkość liter bez znaczenia)
        krs_col = next((col for col in df.columns if 'KRS' in str(col).upper()), None)
        
        if not krs_col:
            # Fallback jeśli nie ma w nagłówku, zwróć pustą listę
            df['clean_krs'] = None
            return df

        # Wyciąganie samego numeru (dokładnie 10 cyfr)
        def extract_krs(text):
            if not text: return None
            match = re.search(r'\b\d{10}\b', str(text).replace(' ', ''))
            return match.group(0) if match else None

        df['clean_krs'] = df[krs_col].apply(extract_krs)
        return df