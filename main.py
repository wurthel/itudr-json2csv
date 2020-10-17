import os
import csv
from dotenv import load_dotenv

import requests
import json
from celery import Celery, shared_task

load_dotenv()

app = Celery('main', broker=os.environ['CELERY_BROKER_URL'])


def run():
    response = requests.get("https://www.berkeleytime.com/api/grades/grades_json/")
    courses = json.loads(response.text)["courses"]
    courses_ids = list(map(lambda c: c["id"], courses))
    courses_ids.sort()

    n = 5
    grouped_ids = [courses_ids[i:i + n] for i in range(0, len(courses_ids), n)]

    cad_file = open("courses_and_departments.csv", "w")
    cad_csv = csv.writer(cad_file)
    cad_csv.writerow(["course_id", "department"])

    incap_file = open("interested_course_and_professors.csv", "w")
    incap_csv = csv.writer(incap_file)
    incap_csv.writerow(["interested_course_id", "professor"])

    for ids in grouped_ids:
        tasks = []
        for id in ids:
            tasks.append(download_departments.delay(id))
            tasks.append(download_professors.delay(id))
        for i, course_id in enumerate(ids):
            department = tasks[2 * i].get()
            cad_csv.writerow([course_id, department["department"]])
            cad_file.flush()

            professors = tasks[2 * i + 1].get()
            for professor in professors:
                incap_csv.writerow([course_id, professor["instructor"]])
                incap_file.flush()


@shared_task
def download_departments(course_id):
    response = requests.get(f"https://www.berkeleytime.com/api/catalog/catalog_json/course_box/?course_id={course_id}")
    info = json.loads(response.text)['course']
    return info


@shared_task
def download_professors(course_id):
    response = requests.get(f"https://www.berkeleytime.com/api/grades/course_grades/{course_id}/")
    info = json.loads(response.text)
    return info


if __name__ == "__main__":
    run()
