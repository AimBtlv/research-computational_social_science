import requests
import pandas as pd
import time

def download_and_save_history(article_title, filename="jackson_wikipedia_history.csv"):
    url = "https://wikipedia.org"
    
    # Очень важно передать User-Agent, иначе Википедия блокирует пустые запросы!
    headers = {
        "User-Agent": "ComputationalSocialScienceResearchBot/1.0 (aima_research@example.com)"
    }
    
    params = {
        "action": "query",
        "prop": "revisions",
        "titles": article_title,
        "rvprop": "timestamp|size|user",
        "rvlimit": "500",
        "format": "json"
    }
    
    revisions_list = []
    page_count = 1
    
    print("Начинаю скачивание истории правок. Пожалуйста, подождите...")
    
    while True:
        try:
            # Передаем и параметры, и заголовки headers
            response = requests.get(url, params=params, headers=headers)
            
            # Проверяем статус ответа сервера
            if response.status_code != 200:
                print(f"Сервер вернул ошибку HTTP: {response.status_code}")
                break
                
            data = response.json()
            
            if 'query' not in data:
                print("Ошибка: В ответе API отсутствуют данные 'query'. Возможно, статья переименована.")
                break
                
            pages = data['query']['pages']
            
            for page_id in pages:
                revisions = pages[page_id].get('revisions', [])
                for rev in revisions:
                    revisions_list.append({
                        'timestamp': rev['timestamp'],
                        'size_bytes': rev['size'],
                        'user': rev.get('user', 'Unknown')
                    })
            
            print(f"Обработано пакетов правок: {page_count} (всего собрано: {len(revisions_list)})")
            
            # Проверяем, есть ли продолжение истории правок
            if 'continue' in data and 'rvcontinue' in data['continue']:
                params['rvcontinue'] = data['continue']['rvcontinue']
                page_count += 1
                time.sleep(0.6) # Небольшая пауза, чтобы не злить серверы Википедии
            else:
                break
        except requests.exceptions.JSONDecodeError:
            print("Ошибка: Сервер Википедии вернул некорректный ответ (не JSON). Вас временно заблокировали за частые запросы.")
            break
        except Exception as e:
            print(f"Произошла непредвиденная ошибка при скачивании: {e}")
            break
            
    if not revisions_list:
        print("Данные не были скачаны. Файл не сохранен.")
        return None
        
    # Переводим в таблицу Pandas
    df = pd.DataFrame(revisions_list)
    
    # Теперь колонка 'timestamp' гарантированно существует
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Сохраняем в CSV файл на компьютер
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"\nУспешно! Все данные сохранены в файл: {filename}")
    print(f"Всего правок в файле: {len(df)}")
    return df

# Запуск функции для статьи о Майкле Джексоне
df_jackson = download_and_save_history("Michael Jackson")