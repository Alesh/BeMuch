# BeMuch
This module help to build multiprocessing tornado apps.
See source code and sample application for more detail.
Sample application is the sharding REST phonebook.
Below sample curl session:
```sh
$ curl http://localhost:8001/phonebook/00124
$ curl -X PUT http://localhost:8003/phonebook/00124 -d "first_name=Iulius&last_name=Caesar"
$ curl http://localhost:8002/phonebook -d "phone=00187&first_name=Marcus&last_name=Antonius"
$ curl http://localhost:8001/phonebook/00124
$ curl http://localhost:8002/phonebook
$ curl -X DELETE http://localhost:8000/phonebook/00124
$ curl http://localhost:8002/phonebook/00124
```

