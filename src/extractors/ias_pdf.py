import pdfplumber
import pandas as pd
import re

class IASPdfExtractor:
    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        
        self.expected_headers = [
            "Numer w rejestrze",
            "Data wpisu",
            "Imię i Nazwisko / Nazwa firmy",
            "Numer KRS",
            "NIP",
            "Rodzaj świadczonych usług",
            "Informacja o zawieszeniu działalności",
            "Informacja o zakończeniu działalności"
        ]

    def extract_table(self) -> pd.DataFrame:
        all_rows = []
        
        with pdfplumber.open(self.pdf_path) as pdf:
            for page in pdf.pages:
                table = page.extract_table()
                if not table:
                    continue
                    
                for row in table:
                    cleaned_row = []
                    for i, cell in enumerate(row):
                        if cell is None:
                            cleaned_row.append("")
                        elif i == 5:
                            cell_text = str(cell).replace('\n', ' ')
                            cell_text = re.sub(r'\s+', ' ', cell_text).strip()
                            cleaned_row.append(cell_text)
                        else:
                            cleaned_row.append(str(cell).replace('\n', ' ').strip())
                            
                    if not any(cleaned_row):
                        continue
                        
                    # Agresywne usuwanie nagłówków
                    first_cell_norm = re.sub(r'\s+', '', cleaned_row[0].upper())
                    second_cell_norm = re.sub(r'\s+', '', cleaned_row[1].upper())
                    
                    if "NUMERWREJESTRZE" in first_cell_norm or "DATAWPISU" in second_cell_norm:
                        continue
                        
                    # POPRAWKA DETEKCJI WIERSZY:
                    # Jeśli nie ma "Numeru w rejestrze" (kol. 0) oraz "KRS" (kol. 3) i "NIP" (kol. 4), 
                    # uznajemy wiersz za obcięty fragment poprzedniego wiersza.
                    is_continuation = (cleaned_row[0] == "" and cleaned_row[3] == "" and cleaned_row[4] == "")
                    
                    if is_continuation and all_rows:
                        # Doklejamy tekst do poprzedniego wiersza, skanując wszystkie kolumny
                        for idx in range(len(cleaned_row)):
                            if cleaned_row[idx]:  # Jeśli fragment zawiera jakikolwiek tekst
                                if all_rows[-1][idx]:
                                    # Dodajemy spację między złączonymi tekstami
                                    all_rows[-1][idx] = (all_rows[-1][idx] + " " + cleaned_row[idx]).strip()
                                else:
                                    # Na wypadek, gdyby poprzednia komórka była wcześniej pusta
                                    all_rows[-1][idx] = cleaned_row[idx].strip()
                    else:
                        # Standardowy wiersz z danymi
                        all_rows.append(cleaned_row)
                        
        if not all_rows:
            raise ValueError("Nie znaleziono tabeli w pliku PDF.")

        df = pd.DataFrame(all_rows)
        
        if len(df.columns) == len(self.expected_headers):
            df.columns = self.expected_headers
        else:
            assigned_headers = self.expected_headers[:len(df.columns)]
            if len(df.columns) > len(self.expected_headers):
                assigned_headers.extend([f"Dodatkowa_{i}" for i in range(len(df.columns) - len(self.expected_headers))])
            df.columns = assigned_headers

        return df

    def clean_krs_column(self, df: pd.DataFrame) -> pd.DataFrame:
        krs_col_name = 'Numer KRS'
        
        if krs_col_name not in df.columns:
            df['clean_krs'] = None
            return df

        def extract_krs(text):
            if pd.isna(text) or not text: 
                return None
            match = re.search(r'\b\d{10}\b', str(text).replace(' ', ''))
            return match.group(0) if match else None

        df['clean_krs'] = df[krs_col_name].apply(extract_krs)
        return df