# -*- coding: utf-8 -*-
"""
Created on Wed May 13 13:49:53 2026

@author: U120137
"""

import requests
import time
import traceback

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

    def parse_krs_json(
        self, 
        json_data: dict
    ) -> dict:
        """Ekstrahuje informacje z Odpisu Pełnego API KRS wraz z datami historycznymi."""
        
        # DODAJEMY FUNKCJĘ CZYSZCZĄCĄ TEKST Z KLAWIATUROWYCH ENTERÓW URZĘDNIKÓW
        def clean_txt(text) -> str:
            if not text:
                return ""
            return " ".join(str(text).split())

        if "error" in json_data:
            return {
                "krs_status": clean_txt(json_data.get("error", "")),
                "likwidacja": "", 
                "krs_adres_aktualny": "", 
                "krs_adresy_historyczne": "",
                "osoby_decyzyjne": "", 
                "historyczne_osoby_decyzyjne": "",
                "udzialowcy": "",
                "historyczni_udzialowcy": ""
            }

        try:
            odpis = json_data.get("odpis", {})
            dane = odpis.get("dane", {})
            naglowekP = odpis.get("naglowekP", {})
            
            wpisy = naglowekP.get("wpis", [])
            data_wpisow_map = {}
            for wpis_slownik in wpisy:
                nr = str(wpis_slownik.get("numerWpisu"))
                data = clean_txt(wpis_slownik.get("dataWpisu", "Brak daty"))
                data_wpisow_map[nr] = data

            def get_data_wpisu(nr_wpisu: str) -> str:
                if not nr_wpisu:
                    return ""
                return data_wpisow_map.get(str(nr_wpisu), f"wpis {nr_wpisu}")

            stan = naglowekP.get("stanPozycji")
            status = "Aktywny" if stan == 1 else "Wykreślony/Inny"

            dzial6 = dane.get("dzial6", {})
            is_liquidated = "Tak" if dzial6 and dzial6.get("rozwiazanieUniewaznienie", {}).get("okreslenieOkolicznosci") else "Nie"

            siedziba_i_adres = dane.get("dzial1", {}).get("siedzibaIAdres", {})
            adres_lista = siedziba_i_adres.get("adres", [])
            
            wszystkie_adresy = []
            for wpis_adresu in adres_lista:
                ulica = clean_txt(wpis_adresu.get("ulica", ""))
                nr_domu = clean_txt(wpis_adresu.get("nrDomu", ""))
                nr_lokalu = clean_txt(wpis_adresu.get("nrLokalu", ""))
                miejscowosc = clean_txt(wpis_adresu.get("miejscowosc", ""))
                kod_pocztowy = clean_txt(wpis_adresu.get("kodPocztowy", ""))
                
                lokal_str = f"/{nr_lokalu}" if nr_lokalu else ""
                ulica_str = f"{ulica} {nr_domu}{lokal_str}".strip() if ulica else f"{nr_domu}{lokal_str}".strip()
                adres_str = f"{ulica_str}, {kod_pocztowy} {miejscowosc}".strip(" ,")
                
                if adres_str and (not wszystkie_adresy or wszystkie_adresy[-1] != adres_str):
                    wszystkie_adresy.append(adres_str)

            krs_adres_aktualny = wszystkie_adresy[-1] if wszystkie_adresy else ""
            krs_adresy_historyczne = " -> ".join(wszystkie_adresy[:-1]) if len(wszystkie_adresy) > 1 else "Brak zmian adresu"

            osoby_decyzyjne = []
            historyczne_osoby_decyzyjne = []
            
            reprezentacja_lista = dane.get("dzial2", {}).get("reprezentacja", [])
            sklad_zarzadu = reprezentacja_lista[-1].get("sklad", []) if reprezentacja_lista else []
            
            for osoba in sklad_zarzadu:
                nazwisko_historia = osoba.get("nazwisko", [])
                if not nazwisko_historia:
                    continue
                
                nr_wprow_zarzad = nazwisko_historia[0].get("nrWpisuWprow", "")
                nr_wykr_zarzad = nazwisko_historia[-1].get("nrWpisuWykr", "")
                data_wprow_zarzad = get_data_wpisu(nr_wpisu=nr_wprow_zarzad)
                
                imie_historia = osoba.get("imiona", [])
                imie = clean_txt(imie_historia[-1].get("imiona", {}).get("imie", "")) if imie_historia else ""
                
                nazwisko_dict = nazwisko_historia[-1].get("nazwisko", {})
                nazwisko = clean_txt(nazwisko_dict.get("nazwiskoICzlon", nazwisko_dict.get("nazwiskoCzlonPierwszy", "")))
                
                identyfikator_historia = osoba.get("identyfikator", [])
                pesel = clean_txt(identyfikator_historia[-1].get("pesel", "")) if identyfikator_historia else ""
                pesel_str = f" (PESEL: {pesel})" if pesel else ""
                
                funkcja_historia = osoba.get("funkcjaWOrganie", [])
                aktualna_funkcja = clean_txt(funkcja_historia[-1].get("funkcjaWOrganie", "Członek organu")) if funkcja_historia else "Członek organu"
                
                osoba_str = f"{imie} {nazwisko}{pesel_str} ({aktualna_funkcja})".strip()

                if nr_wykr_zarzad:
                    data_wykr_zarzad = get_data_wpisu(nr_wpisu=nr_wykr_zarzad)
                    historyczne_osoby_decyzyjne.append(f"{osoba_str} [od {data_wprow_zarzad} do {data_wykr_zarzad}]")
                else:
                    osoby_decyzyjne.append(f"{osoba_str} [od {data_wprow_zarzad}]")

            udzialowcy = []
            historyczni_udzialowcy = []
            
            wspolnicy_lista = dane.get("dzial1", {}).get("wspolnicySpzoo", [])
            
            for wspolnik in wspolnicy_lista:
                nazwa_historia = wspolnik.get("nazwa", [])
                nazwisko_historia = wspolnik.get("nazwisko", [])
                
                udzialy_historia = wspolnik.get("posiadaneUdzialy", [])
                posiadane_udzialy = clean_txt(udzialy_historia[-1].get("posiadaneUdzialy", "")) if udzialy_historia else ""

                if nazwa_historia:
                    nazwa_firmy = clean_txt(nazwa_historia[-1].get("nazwa", ""))
                    nr_wprow_udzialy = nazwa_historia[0].get("nrWpisuWprow", "")
                    nr_wykr_udzialy = nazwa_historia[-1].get("nrWpisuWykr", "")
                    podmiot_str = f"{nazwa_firmy} [{posiadane_udzialy}]"
                elif nazwisko_historia:
                    imie_historia = wspolnik.get("imiona", [])
                    imie = clean_txt(imie_historia[-1].get("imiona", {}).get("imie", "")) if imie_historia else ""
                    
                    nazwisko_dict = nazwisko_historia[-1].get("nazwisko", {})
                    nazwisko = clean_txt(nazwisko_dict.get("nazwiskoICzlon", ""))
                    
                    identyfikator_historia = wspolnik.get("identyfikator", [])
                    pesel = clean_txt(identyfikator_historia[-1].get("pesel", "")) if identyfikator_historia else ""
                    pesel_str = f" (PESEL: {pesel})" if pesel else ""
                    
                    nr_wprow_udzialy = nazwisko_historia[0].get("nrWpisuWprow", "")
                    nr_wykr_udzialy = nazwisko_historia[-1].get("nrWpisuWykr", "")
                    podmiot_str = f"{imie} {nazwisko}{pesel_str} [{posiadane_udzialy}]".strip()
                else:
                    continue
                    
                data_wprow_udzialy = get_data_wpisu(nr_wpisu=nr_wprow_udzialy)

                if nr_wykr_udzialy:
                    data_wykr_udzialy = get_data_wpisu(nr_wpisu=nr_wykr_udzialy)
                    historyczni_udzialowcy.append(f"{podmiot_str} [od {data_wprow_udzialy} do {data_wykr_udzialy}]")
                else:
                    udzialowcy.append(f"{podmiot_str} [od {data_wprow_udzialy}]")

            return {
                "krs_status": status,
                "likwidacja": is_liquidated,
                "krs_adres_aktualny": krs_adres_aktualny,
                "krs_adresy_historyczne": krs_adresy_historyczne,
                "osoby_decyzyjne": " | ".join(osoby_decyzyjne),
                "historyczne_osoby_decyzyjne": " | ".join(historyczne_osoby_decyzyjne),
                "udzialowcy": " | ".join(udzialowcy),
                "historyczni_udzialowcy": " | ".join(historyczni_udzialowcy)
            }

        except Exception as e:
            import traceback
            print(f"BŁĄD PARSOWANIA JSON KRS:\n{traceback.format_exc()}")
            return {
                "krs_status": f"Błąd parsowania",
                "likwidacja": "", 
                "krs_adres_aktualny": "", 
                "krs_adresy_historyczne": "",
                "osoby_decyzyjne": "", 
                "historyczne_osoby_decyzyjne": "",
                "udzialowcy": "",
                "historyczni_udzialowcy": ""
            }