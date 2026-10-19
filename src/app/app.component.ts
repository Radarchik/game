import {Component, EventEmitter, Input, OnDestroy, OnInit, Output} from '@angular/core';
import {AbstractControl, FormArray, FormBuilder, FormGroup, ValidatorFn, Validators} from '@angular/forms';
import {HttpClient} from '@angular/common/http';
import {Question} from './model/question';
import {QuestionsService} from './questions.service';

@Component({
  selector: 'app-root',
  standalone: false,
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements OnInit, OnDestroy {
  startForm: FormGroup;
  questions: Question[];
  rightPassword: string;
  rightPasswordForResult: string;
  public pageToShow: number;

  constructor(private httpClient: HttpClient,
              private questionsService: QuestionsService,
              private formBuilder: FormBuilder) {

  }

  ngOnInit(): void {
    this.rightPassword = (new Date().getMonth() + 1) * 777 + '';
    this.rightPasswordForResult = '7771';
    this.pageToShow = 0;
    this.startForm = this.formBuilder.group({
      password: ['', this.passWordValidator]
    });

    this.httpClient.get('assets/test.json', {responseType: 'json'})
      .subscribe(data => {
        this.questions = [];

        for (let i = 0; i < (data as Array<string>).length; i++) {
          const question = new Question();
          question.id = i + 1;
          question.category = data[i].category;
          question.text = data[i].name;
          question.estimateAuthor = data[i].estimateAuthor;
          question.commentAuthor = data[i].commentAuthor;
          question.isSituation = data[i].isSituation;
          question.isOwn = false;
          this.questions.push(question);
        }

        //перемешиваем вопросы
        this.shuffleQuestions();

        // добавляем собственные вопросы
        const len = this.questions.length + 1;
        for (let i = len; i < len + 3; i++) {
          const question = new Question();
          question.id = i;
          question.category = 'Собственные фантазии (опционально)';
          question.isOwn = true;
          question.text = '';
          this.questions.push(question);
        }
      });


    // this.httpClient.post('https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net', 'Я зашла (Ксения)')
    //   .subscribe(response => {
    //     console.log('');
    //   });
  }

  pageEventHandler($event: any) {
    this.pageToShow = $event;
  }

  ngOnDestroy(): void {
  }


  goToPage1(): void {
    const password = this.startForm.get('password')?.value;

    if (password === this.rightPassword) {
      this.questionsService.questionsReplaySubject.next(this.questions);
      this.pageToShow = 1;
      return;
    }

    if (password === this.rightPasswordForResult) {
      this.httpClient
        .get<Question[]>('assets/result.json')
        .subscribe({
          next: (data) => {
            this.questionsService.questionsReplaySubject.next(data);
            this.pageToShow = 3;
          },
          error: (err) => {
            console.error('Не вдалося завантажити result.json', err);
          }
        });
    }
  }

  passWordValidator(control: AbstractControl) {
    if (control.value !== this?.rightPassword
      || control.value !== this?.rightPasswordForResult) {
      return {wrongPassword: true};
    }
    return null;
  }


  shuffleQuestions(): void {
    for (let i = this.questions.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [this.questions[i], this.questions[j]] = [this.questions[j], this.questions[i]];
    }
  }

}
