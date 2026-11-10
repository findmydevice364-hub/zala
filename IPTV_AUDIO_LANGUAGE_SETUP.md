# IPTV Hunter — аудиоанализ и определение языка

## Что изменено в скрипте

- Исходная структура и все 52 исходные функции сохранены.
- Скрипт расширен с 4 729 до 6 095 строк.
- Дедупликация отключена: каждая запись каждого плейлиста проверяется отдельно, даже если URL полностью совпадает.
- Добавлен отдельный пул из 39 потоков аудиоанализа.
- Определение языка выполняется по звуковому образцу через faster-whisper.
- SpeechBrain VoxLingua107 используется как независимый дополнительный классификатор, если установлен.
- Результаты двух моделей сопоставляются: `CONFIRMED`, `PROVISIONAL`, `CONFLICT`, `LOW_CONFIDENCE`, `UNKNOWN`.
- Извлекаются языковые метки аудиодорожек FFprobe и языки HLS `EXT-X-MEDIA`.
- Все вхождения `stream8.cinerama.uz` принудительно заменяются на `stream1.cinerama.uz` в исходных плейлистах, URL потоков, HLS URI и выходных M3U. Путь, порт и query-параметры сохраняются.
- Исправлена обработка секретов в логах и диагностических отчётах. Рабочие M3U сохраняют исходные query-параметры, необходимые для доступа к потоку.

## Установка зависимостей

В существующий GitHub Actions workflow добавьте после установки базовых Python-зависимостей:

```yaml
- name: Install audio language dependencies
  run: |
    python -m pip install --upgrade pip
    python -m pip install -r requirements-audio.txt
    python -m pip install -r requirements-speechbrain.txt
```

`requirements-audio.txt` устанавливает `faster-whisper`.
`requirements-speechbrain.txt` устанавливает SpeechBrain и его зависимости (включая PyTorch/torchaudio).

Если нужен только Whisper, можно не устанавливать SpeechBrain и задать `IPTV_ENABLE_SPEECHBRAIN=0`.

## Кэш моделей GitHub Actions

Модели скачиваются при первом использовании. Чтобы не загружать их заново при каждом запуске, добавьте перед запуском скрипта:

```yaml
- name: Cache IPTV language models
  uses: actions/cache@v4
  with:
    path: IPTV_REPOSITORY_AUDIT/tools/models
    key: ${{ runner.os }}-iptv-language-models-${{ hashFiles('requirements-audio.txt', 'requirements-speechbrain.txt') }}
    restore-keys: |
      ${{ runner.os }}-iptv-language-models-
```

Не добавляйте `cache: pip` в `actions/setup-python`, если в репозитории по-прежнему нет `requirements.txt` или `pyproject.toml`: именно это ранее вызывало ошибку поиска cache-файла. Для моделей используется отдельный `actions/cache`.

## Параметры по умолчанию

| Переменная | Значение | Назначение |
|---|---:|---|
| `IPTV_AUDIO_LANGUAGE_WORKERS` | `39` | Потоки очереди аудиоанализа |
| `IPTV_AUDIO_EXTRACTION_SLOTS` | `6` | Одновременное декодирование FFmpeg |
| `IPTV_AUDIO_INFERENCE_SLOTS` | `2` | Одновременная работа моделей |
| `IPTV_AUDIO_SAMPLE_SECONDS` | `18` | Длина звукового образца |
| `IPTV_AUDIO_MAX_TRACKS` | `4` | Максимум аудиодорожек на запись |
| `IPTV_WHISPER_MODEL` | `small` | Модель Whisper |
| `IPTV_ENABLE_SPEECHBRAIN` | `1` | Включить SpeechBrain |
| `IPTV_AUDIO_LANGUAGE_ENABLED` | `1` | Включить определение языка |

Для более высокой точности можно установить `IPTV_WHISPER_MODEL=large-v3`, но на CPU это значительно медленнее и требует больше памяти. Для обычного GitHub-hosted runner оставлен `small` плюс независимый SpeechBrain. Пул остаётся 39 потоков; ограничение двух операций инференса защищает runner от исчерпания памяти и CPU.

## Новые выходные файлы

В `IPTV_REPOSITORY_AUDIT/` будут создаваться:

- `Rafail_donor_Ru.m3u` — каналы, классифицированные как российские.
- `Rafail_donor_mir.m3u` — каналы, классифицированные как мировые/зарубежные.
- `Rafail_stable.m3u` — только проверенные рабочие потоки.
- `Language_Summary.txt` — сводка языков и статусов.

В `IPTV_REPOSITORY_AUDIT/channel_database/`:

- `Language_Detection.csv`
- `Language_Detection.json`
- `Audio_Language_Tracks.csv`
- обновлённые `channels.csv`, `channels.json`, `Stream_Tracks.csv`

В `IPTV_REPOSITORY_AUDIT/logs/`:

- `Audio_Language_Audit.log`
- `Audio_Language_Audit.jsonl`

## Важные правила интерпретации

- Язык аудио — это язык реально услышанной речи, а не страна происхождения телеканала.
- Метки FFprobe/HLS сохраняются отдельно от фактического распознавания.
- Если модели расходятся, язык получает статус `CONFLICT`; он не подменяется догадкой.
- При отсутствии речи, звукового образца, FFmpeg или модели результат остаётся `UNKNOWN`.
- Язык сам по себе не доказывает страну канала. Для RU/WORLD сначала учитываются явные country-метаданные, затем группа/название и только затем язык.
- Неизвестные и конфликтующие записи не отправляются автоматически в российскую группу.
- Дубликаты URL не удаляются ни при проверке, ни при формировании базы и M3U.

## Проверки, выполненные перед передачей

- Синтаксическая компиляция Python прошла.
- Проверено сохранение всех 52 исходных функций.
- Проверена принудительная замена домена с сохранением порта, пути и query.
- Проверена обработка секретов.
- Проверена HLS-метка языка аудиодорожки.
- Проверено, что две одинаковые URL-записи проверяются и сохраняются как две отдельные записи.
- Выполнен тест формирования новых плейлистов и отчётов на искусственных тестовых данных.

Проверки не заменяют реальный запуск на GitHub Actions: фактическое распознавание зависит от доступности потоков, наличия речи в образце, установки FFmpeg и скачивания моделей.
