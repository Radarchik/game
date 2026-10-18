import {Component, EventEmitter, OnInit, Output} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {from, Observable} from 'rxjs';

@Component({
  selector: 'app-page-result',
  standalone: false,
  templateUrl: './page-result.component.html',
  styleUrls: ['./page-result.component.css']
})
export class PageResultComponent implements OnInit {
  @Output() pageToShowEvent = new EventEmitter<number>();
  private authToken = '76c030ab71daae5908437a13af8bec91';
  constructor(private httpClient: HttpClient) { }

  ngOnInit() {
    this.getData().subscribe(data => {
      console.log('profiles2', data);
    });
    //  this.getProfiles1();
     // this.getProfiles2();
  }


  private getProfiles1() {
    const headers = new HttpHeaders({
      authorization: 'Bearer 76c030ab71daae5908437a13af8bec91',
      'Content-Type': 'application/json',
    });

    this.httpClient.get('https://api.pipedream.com/v1/sources/dc_lVunxp4/event_summaries?expand=event'
      , { headers })
      .subscribe(data => {
        console.log('profiles4', data);
      });
/*    this.httpClient.get('https://thingproxy.freeboard.io/fetch/https://api.pipedream.com/v1/sources/dc_K0uwzaE'
      , { headers })
      .subscribe(data => {
        console.log('profiles5', data);
      });*/
  }

  getData(): Observable<any> {
    return from(
      fetch(
        'https://api.pipedream.com/v1/sources/dc_lVunxp4/event_summaries?expand=event', // the url you are trying to access
        {
/*          headers: {
            authorization: 'Bearer 76c030ab71daae5908437a13af8bec91',
            'Access-Control-Allow-Credentials': 'true'
          },*/
          method: 'GET', // GET, POST, PUT, DELETE
         // mode: 'no-cors' // the most important option
        }
      ));
  }

  private getProfiles2() {
    const http = new XMLHttpRequest();

    http.open('GET', 'https://api.pipedream.com/v1/sources/dc_lVunxp4/event_summaries?expand=event');
    http.setRequestHeader('authorization', 'Bearer 76c030ab71daae5908437a13af8bec91');
    http.send();

    http.onload = () => console.log(http.responseText);
  }
}
