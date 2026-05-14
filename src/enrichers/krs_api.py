# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:49:53 2026

@author: U120137
"""

import requests
import time

class KrsApiEnricher:
    # Zmiana z OdpisAktualny na OdpisPelny (daje dostęp do historii zmian)
    BASE_URL = "https://api-krs.ms.gov.pl/api/krs/OdpisPelny"

    def fetch_entity_data(self, krs_number: str) -> dict:
        url = f"{self.BASE_URL}/{krs_number}?rejestr=P&format=json"
        
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return {"error": "Nie znaleziono w KRS (być może wykreślony lub inny rejestr)"}
            else:
                return {"error": f"Błąd API: {response.status_code}"}
        except requests.RequestException as e:
            return {"error": str(e)}

    def parse_krs_json(self, json_data: dict) -> dict:
        if "error" in json_data:
            return {
                "krs_status": json_data["error"],
                "likwidacja": "", "krs_adres_aktualny": "", "krs_adresy_historyczne": "",
                "osoby_decyzyjne": "", "udzialowcy": ""
            }

        try:
            odpis = json_data.get("odpis", {})
            dane = odpis.get("dane", {})
            
            # 1. Status podmiotu
            stan = odpis.get("naglowekA", {}).get("stanPozycji")
            status = "Aktywny" if stan == 1 else "Wykreślony/Inny"

            # 2. Informacja o likwidacji
            dzial6 = dane.get("dzial6", {})
            is_liquidated = "Tak" if dzial6 else "Nie"

            # 3. HISTORIA ADRESÓW (Odpis Pełny zwraca listę wpisów)
            siedziba_i_adres = dane.get("dzial1", {}).get("siedzibaIAdres", [])
            # Upewniamy się, że to lista (dla spójności)
            if isinstance(siedziba_i_adres, dict):
                siedziba_i_adres = [siedziba_i_adres]

            wszystkie_adresy = []
            for wpis in siedziba_i_adres:
                adres_data = wpis.get("adres", {})
                ulica = adres_data.get("ulica", "")
                nr_domu = adres_data.get("nrDomu", "")
                nr_lokalu = adres_data.get("nrLokalu", "")
                miejscowosc = adres_data.get("miejscowosc", "")
                kod_pocztowy = adres_data.get("kodPocztowy", "")
                
                lokal_str = f"/{nr_lokalu}" if nr_lokalu else ""
                ulica_str = f"{ulica} {nr_domu}{lokal_str}".strip() if ulica else f"{nr_domu}{lokal_str}".strip()
                adres_str = f"{ulica_str}, {kod_pocztowy} {miejscowosc}".strip(" ,")
                
                # Dodajemy tylko unikalne adresy, by uniknąć duplikatów przy aneksach bez zmiany adresu
                if adres_str and (not wszystkie_adresy or wszystkie_adresy[-1] != adres_str):
                    wszystkie_adresy.append(adres_str)

            # Rozdzielenie na adres aktualny (zawsze ostatni na liście w KRS) i historię
            krs_adres_aktualny = wszystkie_adresy[-1] if wszystkie_adresy else ""
            
            # Jeśli jest więcej niż 1 adres, formatujemy historię jako: "Adres 1 -> Adres 2 -> ..."
            krs_adresy_historyczne = " -> ".join(wszystkie_adresy[:-1]) if len(wszystkie_adresy) > 1 else "Brak zmian adresu"

            # 4. Osoby decyzyjne (wyciągamy tylko z najświeższych wpisów, by uprościć - dla Odpisu Pełnego to zazwyczaj ostatni element listy reprezentacji)
            osoby_decyzyjne = []
            reprezentacja_lista = dane.get("dzial2", {}).get("reprezentacja", [])
            if isinstance(reprezentacja_lista, dict):
                reprezentacja_lista = [reprezentacja_lista]
                
            # Bierzemy ostatni (najbardziej aktualny) wpis o reprezentacji
            sklad_zarzadu = reprezentacja_lista[-1].get("sklad", []) if reprezentacja_lista else []
            for osoba in sklad_zarzadu:
                # W Odpisie Pełnym osoby usunięte z zarządu mogą mieć znacznik wykreślenia
                if "informacjaOWykresleniu" in osoba:
                    continue # Pomijamy byłych członków zarządu
                    
                imie = osoba.get("imiona", {}).get("imie", "")
                nazwisko = osoba.get("nazwisko", {}).get("nazwiskoICzlon", osoba.get("nazwisko", {}).get("nazwiskoCzlonPierwszy", ""))
                funkcja = osoba.get("funkcjaWOrganie", "Członek organu")
                osoby_decyzyjne.append(f"{imie} {nazwisko} ({funkcja})".strip())

            # 5. Udziałowcy (też bierzemy najbardziej aktualnych, ignorując wykreślonych)
            udzialowcy = []
            wspolnicy_lista = dane.get("dzial1", {}).get("wspolnicySpzoo", [])
            if isinstance(wspolnicy_lista, dict):
                wspolnicy_lista = [wspolnicy_lista]
                
            for w in wspolnicy_lista:
                if "informacjaOWykresleniu" in w:
                    continue # Pomijamy starych udziałowców (sprzedawców "spółki z półki")
                    
                posiadane_udzialy = w.get("posiadaneUdzialy", "")
                nazwa_firmy = w.get("nazwa", "")
                
                if nazwa_firmy:
                    udzialowcy.append(f"{nazwa_firmy} [{posiadane_udzialy}]")
                else:
                    imie = w.get("imiona", {}).get("imie", "")
                    nazwisko = w.get("nazwisko", {}).get("nazwiskoICzlon", "")
                    udzialowcy.append(f"{imie} {nazwisko} [{posiadane_udzialy}]".strip())

            return {
                "krs_status": status,
                "likwidacja": is_liquidated,
                "krs_adres_aktualny": krs_adres_aktualny,
                "krs_adresy_historyczne": krs_adresy_historyczne,
                "osoby_decyzyjne": " | ".join(osoby_decyzyjne),
                "udzialowcy": " | ".join(udzialowcy)
            }

        except Exception as e:
            return {
                "krs_status": f"Błąd parsowania: {str(e)}",
                "likwidacja": "", "krs_adres_aktualny": "", "krs_adresy_historyczne": "",
                "osoby_decyzyjne": "", "udzialowcy": ""
            }