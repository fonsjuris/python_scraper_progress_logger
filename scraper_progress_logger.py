from python_mysql_dal import dal

_SCRAPER_STARTED_STATUS_ID = 1
_SCRAPER_FINISHED_STATUS_ID = 2
_SCRAPER_ERROR_STATUS_ID = 3


def start_scraper(scraper_id):
    scraper_run_id = dal.save('scraper_run',
             ('scraper_id', 'status_id', 'num_processed_items'),
             (scraper_id, _SCRAPER_STARTED_STATUS_ID, 0))
    return scraper_run_id


def scraper_finished(scraper_run_id, num_items_processed):
    dal.update('scraper_run',
               ('status_id', 'num_processed_items'),
               (_SCRAPER_FINISHED_STATUS_ID, num_items_processed),
               f'id={scraper_run_id}')
