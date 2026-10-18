var yk=Object.defineProperty,wk=Object.defineProperties;var Ck=Object.getOwnPropertyDescriptors;var xl=Object.getOwnPropertySymbols;var _v=Object.prototype.hasOwnProperty,vv=Object.prototype.propertyIsEnumerable;var gv=(n,t,e)=>t in n?yk(n,t,{enumerable:!0,configurable:!0,writable:!0,value:e}):n[t]=e,M=(n,t)=>{for(var e in t||={})_v.call(t,e)&&gv(n,e,t[e]);if(xl)for(var e of xl(t))vv.call(t,e)&&gv(n,e,t[e]);return n},be=(n,t)=>wk(n,Ck(t));var Am=(n,t)=>{var e={};for(var i in n)_v.call(n,i)&&t.indexOf(i)<0&&(e[i]=n[i]);if(n!=null&&xl)for(var i of xl(n))t.indexOf(i)<0&&vv.call(n,i)&&(e[i]=n[i]);return e};var wi=(n,t,e)=>new Promise((i,r)=>{var o=l=>{try{a(e.next(l))}catch(c){r(c)}},s=l=>{try{a(e.throw(l))}catch(c){r(c)}},a=l=>l.done?i(l.value):Promise.resolve(l.value).then(o,s);a((e=e.apply(n,t)).next())});var _t=null,El=!1,Rm=1,Dk=null,lt=Symbol("SIGNAL");function K(n){let t=_t;return _t=n,t}function Ml(){return _t}var sr={version:0,lastCleanEpoch:0,dirty:!1,producers:void 0,producersTail:void 0,consumers:void 0,consumersTail:void 0,recomputing:!1,consumerAllowSignalWrites:!1,consumerIsAlwaysLive:!1,kind:"unknown",producerMustRecompute:()=>!1,producerRecomputeValue:()=>{},consumerMarkedDirty:()=>{},consumerOnSignalRead:()=>{}};function no(n){if(El)throw new Error("");if(_t===null)return;_t.consumerOnSignalRead(n);let t=_t.producersTail;if(t!==void 0&&t.producer===n)return;let e,i=_t.recomputing;if(i&&(e=t!==void 0?t.nextProducer:_t.producers,e!==void 0&&e.producer===n)){_t.producersTail=e,e.lastReadVersion=n.version;return}let r=n.consumersTail;if(r!==void 0&&r.consumer===_t&&(!i||Ek(r,_t)))return;let o=ro(_t),s={producer:n,consumer:_t,nextProducer:e,prevConsumer:r,lastReadVersion:n.version,nextConsumer:void 0};_t.producersTail=s,t!==void 0?t.nextProducer=s:_t.producers=s,o&&Cv(n,s)}function bv(){Rm++}function Tl(n){if(!(ro(n)&&!n.dirty)&&!(!n.dirty&&n.lastCleanEpoch===Rm)){if(!n.producerMustRecompute(n)&&!io(n)){Il(n);return}n.producerRecomputeValue(n),Il(n)}}function Om(n){if(n.consumers===void 0)return;let t=El;El=!0;try{for(let e=n.consumers;e!==void 0;e=e.nextConsumer){let i=e.consumer;i.dirty||xk(i)}}finally{El=t}}function Nm(){return _t?.consumerAllowSignalWrites!==!1}function xk(n){n.dirty=!0,Om(n),n.consumerMarkedDirty?.(n)}function Il(n){n.dirty=!1,n.lastCleanEpoch=Rm}function Ci(n){return n&&yv(n),K(n)}function yv(n){n.producersTail=void 0,n.recomputing=!0}function ar(n,t){K(t),n&&wv(n)}function wv(n){n.recomputing=!1;let t=n.producersTail,e=t!==void 0?t.nextProducer:n.producers;if(e!==void 0){if(ro(n))do e=Fm(e);while(e!==void 0);t!==void 0?t.nextProducer=void 0:n.producers=void 0}}function io(n){for(let t=n.producers;t!==void 0;t=t.nextProducer){let e=t.producer,i=t.lastReadVersion;if(i!==e.version||(Tl(e),i!==e.version))return!0}return!1}function Di(n){if(ro(n)){let t=n.producers;for(;t!==void 0;)t=Fm(t)}n.producers=void 0,n.producersTail=void 0,n.consumers=void 0,n.consumersTail=void 0}function Cv(n,t){let e=n.consumersTail,i=ro(n);if(e!==void 0?(t.nextConsumer=e.nextConsumer,e.nextConsumer=t):(t.nextConsumer=void 0,n.consumers=t),t.prevConsumer=e,n.consumersTail=t,!i)for(let r=n.producers;r!==void 0;r=r.nextProducer)Cv(r.producer,r)}function Fm(n){let t=n.producer,e=n.nextProducer,i=n.nextConsumer,r=n.prevConsumer;if(n.nextConsumer=void 0,n.prevConsumer=void 0,i!==void 0?i.prevConsumer=r:t.consumersTail=r,r!==void 0)r.nextConsumer=i;else if(t.consumers=i,!ro(t)){let o=t.producers;for(;o!==void 0;)o=Fm(o)}return e}function ro(n){return n.consumerIsAlwaysLive||n.consumers!==void 0}function Al(n){Dk?.(n)}function Ek(n,t){let e=t.producersTail;if(e!==void 0){let i=t.producers;do{if(i===n)return!0;if(i===e)break;i=i.nextProducer}while(i!==void 0)}return!1}function Rl(n,t){return Object.is(n,t)}function ys(n,t){let e=Object.create(kk);e.computation=n,t!==void 0&&(e.equal=t);let i=()=>{if(Tl(e),no(e),e.value===bs)throw e.error;return e.value};return i[lt]=e,Al(e),i}var kl=Symbol("UNSET"),Sl=Symbol("COMPUTING"),bs=Symbol("ERRORED"),kk=be(M({},sr),{value:kl,dirty:!0,error:null,equal:Rl,kind:"computed",producerMustRecompute(n){return n.value===kl||n.value===Sl},producerRecomputeValue(n){if(n.value===Sl)throw new Error("");let t=n.value;n.value=Sl;let e=Ci(n),i,r=!1;try{i=n.computation(),K(null),r=t!==kl&&t!==bs&&i!==bs&&n.equal(t,i)}catch(o){i=bs,n.error=o}finally{ar(n,e)}if(r){n.value=t;return}n.value=i,n.version++}});function Sk(){throw new Error}var Dv=Sk;function xv(n){Dv(n)}function Pm(n){Dv=n}var Ik=null;function Lm(n,t){let e=Object.create(Ol);e.value=n,t!==void 0&&(e.equal=t);let i=()=>Ev(e);return i[lt]=e,Al(e),[i,s=>ws(e,s),s=>Bm(e,s)]}function Ev(n){return no(n),n.value}function ws(n,t){Nm()||xv(n),n.equal(n.value,t)||(n.value=t,Mk(n))}function Bm(n,t){Nm()||xv(n),ws(n,t(n.value))}var Ol=be(M({},sr),{equal:Rl,value:void 0,kind:"signal"});function Mk(n){n.version++,bv(),Om(n),Ik?.(n)}var Vm=be(M({},sr),{consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,dirty:!0,kind:"effect"});function jm(n){if(n.dirty=!1,n.version>0&&!io(n))return;n.version++;let t=Ci(n);try{n.cleanup(),n.fn()}finally{ar(n,t)}}function se(n){return typeof n=="function"}function Nl(n){let e=n(i=>{Error.call(i),i.stack=new Error().stack});return e.prototype=Object.create(Error.prototype),e.prototype.constructor=e,e}var Fl=Nl(n=>function(e){n(this),this.message=e?`${e.length} errors occurred during unsubscription:
${e.map((i,r)=>`${r+1}) ${i.toString()}`).join(`
  `)}`:"",this.name="UnsubscriptionError",this.errors=e});function lr(n,t){if(n){let e=n.indexOf(t);0<=e&&n.splice(e,1)}}var ge=class n{constructor(t){this.initialTeardown=t,this.closed=!1,this._parentage=null,this._finalizers=null}unsubscribe(){let t;if(!this.closed){this.closed=!0;let{_parentage:e}=this;if(e)if(this._parentage=null,Array.isArray(e))for(let o of e)o.remove(this);else e.remove(this);let{initialTeardown:i}=this;if(se(i))try{i()}catch(o){t=o instanceof Fl?o.errors:[o]}let{_finalizers:r}=this;if(r){this._finalizers=null;for(let o of r)try{kv(o)}catch(s){t=t??[],s instanceof Fl?t=[...t,...s.errors]:t.push(s)}}if(t)throw new Fl(t)}}add(t){var e;if(t&&t!==this)if(this.closed)kv(t);else{if(t instanceof n){if(t.closed||t._hasParent(this))return;t._addParent(this)}(this._finalizers=(e=this._finalizers)!==null&&e!==void 0?e:[]).push(t)}}_hasParent(t){let{_parentage:e}=this;return e===t||Array.isArray(e)&&e.includes(t)}_addParent(t){let{_parentage:e}=this;this._parentage=Array.isArray(e)?(e.push(t),e):e?[e,t]:t}_removeParent(t){let{_parentage:e}=this;e===t?this._parentage=null:Array.isArray(e)&&lr(e,t)}remove(t){let{_finalizers:e}=this;e&&lr(e,t),t instanceof n&&t._removeParent(this)}};ge.EMPTY=(()=>{let n=new ge;return n.closed=!0,n})();var zm=ge.EMPTY;function Pl(n){return n instanceof ge||n&&"closed"in n&&se(n.remove)&&se(n.add)&&se(n.unsubscribe)}function kv(n){se(n)?n():n.unsubscribe()}var un={onUnhandledError:null,onStoppedNotification:null,Promise:void 0,useDeprecatedSynchronousErrorHandling:!1,useDeprecatedNextContext:!1};var oo={setTimeout(n,t,...e){let{delegate:i}=oo;return i?.setTimeout?i.setTimeout(n,t,...e):setTimeout(n,t,...e)},clearTimeout(n){let{delegate:t}=oo;return(t?.clearTimeout||clearTimeout)(n)},delegate:void 0};function Ll(n){oo.setTimeout(()=>{let{onUnhandledError:t}=un;if(t)t(n);else throw n})}function Cs(){}var Sv=Hm("C",void 0,void 0);function Iv(n){return Hm("E",void 0,n)}function Mv(n){return Hm("N",n,void 0)}function Hm(n,t,e){return{kind:n,value:t,error:e}}var cr=null;function so(n){if(un.useDeprecatedSynchronousErrorHandling){let t=!cr;if(t&&(cr={errorThrown:!1,error:null}),n(),t){let{errorThrown:e,error:i}=cr;if(cr=null,e)throw i}}else n()}function Tv(n){un.useDeprecatedSynchronousErrorHandling&&cr&&(cr.errorThrown=!0,cr.error=n)}var dr=class extends ge{constructor(t){super(),this.isStopped=!1,t?(this.destination=t,Pl(t)&&t.add(this)):this.destination=Rk}static create(t,e,i){return new Kn(t,e,i)}next(t){this.isStopped?$m(Mv(t),this):this._next(t)}error(t){this.isStopped?$m(Iv(t),this):(this.isStopped=!0,this._error(t))}complete(){this.isStopped?$m(Sv,this):(this.isStopped=!0,this._complete())}unsubscribe(){this.closed||(this.isStopped=!0,super.unsubscribe(),this.destination=null)}_next(t){this.destination.next(t)}_error(t){try{this.destination.error(t)}finally{this.unsubscribe()}}_complete(){try{this.destination.complete()}finally{this.unsubscribe()}}},Tk=Function.prototype.bind;function Um(n,t){return Tk.call(n,t)}var Gm=class{constructor(t){this.partialObserver=t}next(t){let{partialObserver:e}=this;if(e.next)try{e.next(t)}catch(i){Bl(i)}}error(t){let{partialObserver:e}=this;if(e.error)try{e.error(t)}catch(i){Bl(i)}else Bl(t)}complete(){let{partialObserver:t}=this;if(t.complete)try{t.complete()}catch(e){Bl(e)}}},Kn=class extends dr{constructor(t,e,i){super();let r;if(se(t)||!t)r={next:t??void 0,error:e??void 0,complete:i??void 0};else{let o;this&&un.useDeprecatedNextContext?(o=Object.create(t),o.unsubscribe=()=>this.unsubscribe(),r={next:t.next&&Um(t.next,o),error:t.error&&Um(t.error,o),complete:t.complete&&Um(t.complete,o)}):r=t}this.destination=new Gm(r)}};function Bl(n){un.useDeprecatedSynchronousErrorHandling?Tv(n):Ll(n)}function Ak(n){throw n}function $m(n,t){let{onStoppedNotification:e}=un;e&&oo.setTimeout(()=>e(n,t))}var Rk={closed:!0,next:Cs,error:Ak,complete:Cs};var ao=typeof Symbol=="function"&&Symbol.observable||"@@observable";function mn(n){return n}function Av(n){return n.length===0?mn:n.length===1?n[0]:function(e){return n.reduce((i,r)=>r(i),e)}}var ae=(()=>{class n{constructor(e){e&&(this._subscribe=e)}lift(e){let i=new n;return i.source=this,i.operator=e,i}subscribe(e,i,r){let o=Nk(e)?e:new Kn(e,i,r);return so(()=>{let{operator:s,source:a}=this;o.add(s?s.call(o,a):a?this._subscribe(o):this._trySubscribe(o))}),o}_trySubscribe(e){try{return this._subscribe(e)}catch(i){e.error(i)}}forEach(e,i){return i=Rv(i),new i((r,o)=>{let s=new Kn({next:a=>{try{e(a)}catch(l){o(l),s.unsubscribe()}},error:o,complete:r});this.subscribe(s)})}_subscribe(e){var i;return(i=this.source)===null||i===void 0?void 0:i.subscribe(e)}[ao](){return this}pipe(...e){return Av(e)(this)}toPromise(e){return e=Rv(e),new e((i,r)=>{let o;this.subscribe(s=>o=s,s=>r(s),()=>i(o))})}}return n.create=t=>new n(t),n})();function Rv(n){var t;return(t=n??un.Promise)!==null&&t!==void 0?t:Promise}function Ok(n){return n&&se(n.next)&&se(n.error)&&se(n.complete)}function Nk(n){return n&&n instanceof dr||Ok(n)&&Pl(n)}function Wm(n){return se(n?.lift)}function he(n){return t=>{if(Wm(t))return t.lift(function(e){try{return n(e,this)}catch(i){this.error(i)}});throw new TypeError("Unable to lift unknown Observable type")}}function ue(n,t,e,i,r){return new qm(n,t,e,i,r)}var qm=class extends dr{constructor(t,e,i,r,o,s){super(t),this.onFinalize=o,this.shouldUnsubscribe=s,this._next=e?function(a){try{e(a)}catch(l){t.error(l)}}:super._next,this._error=r?function(a){try{r(a)}catch(l){t.error(l)}finally{this.unsubscribe()}}:super._error,this._complete=i?function(){try{i()}catch(a){t.error(a)}finally{this.unsubscribe()}}:super._complete}unsubscribe(){var t;if(!this.shouldUnsubscribe||this.shouldUnsubscribe()){let{closed:e}=this;super.unsubscribe(),!e&&((t=this.onFinalize)===null||t===void 0||t.call(this))}}};function Ov(){return he((n,t)=>{let e=null;n._refCount++;let i=ue(t,void 0,void 0,void 0,()=>{if(!n||n._refCount<=0||0<--n._refCount){e=null;return}let r=n._connection,o=e;e=null,r&&(!o||r===o)&&r.unsubscribe(),t.unsubscribe()});n.subscribe(i),i.closed||(e=n.connect())})}var Ds=class extends ae{constructor(t,e){super(),this.source=t,this.subjectFactory=e,this._subject=null,this._refCount=0,this._connection=null,Wm(t)&&(this.lift=t.lift)}_subscribe(t){return this.getSubject().subscribe(t)}getSubject(){let t=this._subject;return(!t||t.isStopped)&&(this._subject=this.subjectFactory()),this._subject}_teardown(){this._refCount=0;let{_connection:t}=this;this._subject=this._connection=null,t?.unsubscribe()}connect(){let t=this._connection;if(!t){t=this._connection=new ge;let e=this.getSubject();t.add(this.source.subscribe(ue(e,void 0,()=>{this._teardown(),e.complete()},i=>{this._teardown(),e.error(i)},()=>this._teardown()))),t.closed&&(this._connection=null,t=ge.EMPTY)}return t}refCount(){return Ov()(this)}};var lo={schedule(n){let t=requestAnimationFrame,e=cancelAnimationFrame,{delegate:i}=lo;i&&(t=i.requestAnimationFrame,e=i.cancelAnimationFrame);let r=t(o=>{e=void 0,n(o)});return new ge(()=>e?.(r))},requestAnimationFrame(...n){let{delegate:t}=lo;return(t?.requestAnimationFrame||requestAnimationFrame)(...n)},cancelAnimationFrame(...n){let{delegate:t}=lo;return(t?.cancelAnimationFrame||cancelAnimationFrame)(...n)},delegate:void 0};var Nv=Nl(n=>function(){n(this),this.name="ObjectUnsubscribedError",this.message="object unsubscribed"});var k=(()=>{class n extends ae{constructor(){super(),this.closed=!1,this.currentObservers=null,this.observers=[],this.isStopped=!1,this.hasError=!1,this.thrownError=null}lift(e){let i=new Vl(this,this);return i.operator=e,i}_throwIfClosed(){if(this.closed)throw new Nv}next(e){so(()=>{if(this._throwIfClosed(),!this.isStopped){this.currentObservers||(this.currentObservers=Array.from(this.observers));for(let i of this.currentObservers)i.next(e)}})}error(e){so(()=>{if(this._throwIfClosed(),!this.isStopped){this.hasError=this.isStopped=!0,this.thrownError=e;let{observers:i}=this;for(;i.length;)i.shift().error(e)}})}complete(){so(()=>{if(this._throwIfClosed(),!this.isStopped){this.isStopped=!0;let{observers:e}=this;for(;e.length;)e.shift().complete()}})}unsubscribe(){this.isStopped=this.closed=!0,this.observers=this.currentObservers=null}get observed(){var e;return((e=this.observers)===null||e===void 0?void 0:e.length)>0}_trySubscribe(e){return this._throwIfClosed(),super._trySubscribe(e)}_subscribe(e){return this._throwIfClosed(),this._checkFinalizedStatuses(e),this._innerSubscribe(e)}_innerSubscribe(e){let{hasError:i,isStopped:r,observers:o}=this;return i||r?zm:(this.currentObservers=null,o.push(e),new ge(()=>{this.currentObservers=null,lr(o,e)}))}_checkFinalizedStatuses(e){let{hasError:i,thrownError:r,isStopped:o}=this;i?e.error(r):o&&e.complete()}asObservable(){let e=new ae;return e.source=this,e}}return n.create=(t,e)=>new Vl(t,e),n})(),Vl=class extends k{constructor(t,e){super(),this.destination=t,this.source=e}next(t){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.next)===null||i===void 0||i.call(e,t)}error(t){var e,i;(i=(e=this.destination)===null||e===void 0?void 0:e.error)===null||i===void 0||i.call(e,t)}complete(){var t,e;(e=(t=this.destination)===null||t===void 0?void 0:t.complete)===null||e===void 0||e.call(t)}_subscribe(t){var e,i;return(i=(e=this.source)===null||e===void 0?void 0:e.subscribe(t))!==null&&i!==void 0?i:zm}};var Bt=class extends k{constructor(t){super(),this._value=t}get value(){return this.getValue()}_subscribe(t){let e=super._subscribe(t);return!e.closed&&t.next(this._value),e}getValue(){let{hasError:t,thrownError:e,_value:i}=this;if(t)throw e;return this._throwIfClosed(),i}next(t){super.next(this._value=t)}};var xs={now(){return(xs.delegate||Date).now()},delegate:void 0};var Kt=class extends k{constructor(t=1/0,e=1/0,i=xs){super(),this._bufferSize=t,this._windowTime=e,this._timestampProvider=i,this._buffer=[],this._infiniteTimeWindow=!0,this._infiniteTimeWindow=e===1/0,this._bufferSize=Math.max(1,t),this._windowTime=Math.max(1,e)}next(t){let{isStopped:e,_buffer:i,_infiniteTimeWindow:r,_timestampProvider:o,_windowTime:s}=this;e||(i.push(t),!r&&i.push(o.now()+s)),this._trimBuffer(),super.next(t)}_subscribe(t){this._throwIfClosed(),this._trimBuffer();let e=this._innerSubscribe(t),{_infiniteTimeWindow:i,_buffer:r}=this,o=r.slice();for(let s=0;s<o.length&&!t.closed;s+=i?1:2)t.next(o[s]);return this._checkFinalizedStatuses(t),e}_trimBuffer(){let{_bufferSize:t,_timestampProvider:e,_buffer:i,_infiniteTimeWindow:r}=this,o=(r?1:2)*t;if(t<1/0&&o<i.length&&i.splice(0,i.length-o),!r){let s=e.now(),a=0;for(let l=1;l<i.length&&i[l]<=s;l+=2)a=l;a&&i.splice(0,a+1)}}};var jl=class extends ge{constructor(t,e){super()}schedule(t,e=0){return this}};var Es={setInterval(n,t,...e){let{delegate:i}=Es;return i?.setInterval?i.setInterval(n,t,...e):setInterval(n,t,...e)},clearInterval(n){let{delegate:t}=Es;return(t?.clearInterval||clearInterval)(n)},delegate:void 0};var xi=class extends jl{constructor(t,e){super(t,e),this.scheduler=t,this.work=e,this.pending=!1}schedule(t,e=0){var i;if(this.closed)return this;this.state=t;let r=this.id,o=this.scheduler;return r!=null&&(this.id=this.recycleAsyncId(o,r,e)),this.pending=!0,this.delay=e,this.id=(i=this.id)!==null&&i!==void 0?i:this.requestAsyncId(o,this.id,e),this}requestAsyncId(t,e,i=0){return Es.setInterval(t.flush.bind(t,this),i)}recycleAsyncId(t,e,i=0){if(i!=null&&this.delay===i&&this.pending===!1)return e;e!=null&&Es.clearInterval(e)}execute(t,e){if(this.closed)return new Error("executing a cancelled action");this.pending=!1;let i=this._execute(t,e);if(i)return i;this.pending===!1&&this.id!=null&&(this.id=this.recycleAsyncId(this.scheduler,this.id,null))}_execute(t,e){let i=!1,r;try{this.work(t)}catch(o){i=!0,r=o||new Error("Scheduled action threw falsy error")}if(i)return this.unsubscribe(),r}unsubscribe(){if(!this.closed){let{id:t,scheduler:e}=this,{actions:i}=e;this.work=this.state=this.scheduler=null,this.pending=!1,lr(i,this),t!=null&&(this.id=this.recycleAsyncId(e,t,null)),this.delay=null,super.unsubscribe()}}};var Fk=1,Qm,Km={};function Fv(n){return n in Km?(delete Km[n],!0):!1}var Pv={setImmediate(n){let t=Fk++;return Km[t]=!0,Qm||(Qm=Promise.resolve()),Qm.then(()=>Fv(t)&&n()),t},clearImmediate(n){Fv(n)}};var{setImmediate:Pk,clearImmediate:Lk}=Pv,ks={setImmediate(...n){let{delegate:t}=ks;return(t?.setImmediate||Pk)(...n)},clearImmediate(n){let{delegate:t}=ks;return(t?.clearImmediate||Lk)(n)},delegate:void 0};var zl=class extends xi{constructor(t,e){super(t,e),this.scheduler=t,this.work=e}requestAsyncId(t,e,i=0){return i!==null&&i>0?super.requestAsyncId(t,e,i):(t.actions.push(this),t._scheduled||(t._scheduled=ks.setImmediate(t.flush.bind(t,void 0))))}recycleAsyncId(t,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(t,e,i);let{actions:o}=t;e!=null&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(ks.clearImmediate(e),t._scheduled===e&&(t._scheduled=void 0))}};var co=class n{constructor(t,e=n.now){this.schedulerActionCtor=t,this.now=e}schedule(t,e=0,i){return new this.schedulerActionCtor(this,t).schedule(i,e)}};co.now=xs.now;var Ei=class extends co{constructor(t,e=co.now){super(t,e),this.actions=[],this._active=!1}flush(t){let{actions:e}=this;if(this._active){e.push(t);return}let i;this._active=!0;do if(i=t.execute(t.state,t.delay))break;while(t=e.shift());if(this._active=!1,i){for(;t=e.shift();)t.unsubscribe();throw i}}};var Hl=class extends Ei{flush(t){this._active=!0;let e=this._scheduled;this._scheduled=void 0;let{actions:i}=this,r;t=t||i.shift();do if(r=t.execute(t.state,t.delay))break;while((t=i[0])&&t.id===e&&i.shift());if(this._active=!1,r){for(;(t=i[0])&&t.id===e&&i.shift();)t.unsubscribe();throw r}}};var Ul=new Hl(zl);var Ss=new Ei(xi),Lv=Ss;var $l=class extends xi{constructor(t,e){super(t,e),this.scheduler=t,this.work=e}requestAsyncId(t,e,i=0){return i!==null&&i>0?super.requestAsyncId(t,e,i):(t.actions.push(this),t._scheduled||(t._scheduled=lo.requestAnimationFrame(()=>t.flush(void 0))))}recycleAsyncId(t,e,i=0){var r;if(i!=null?i>0:this.delay>0)return super.recycleAsyncId(t,e,i);let{actions:o}=t;e!=null&&e===t._scheduled&&((r=o[o.length-1])===null||r===void 0?void 0:r.id)!==e&&(lo.cancelAnimationFrame(e),t._scheduled=void 0)}};var Gl=class extends Ei{flush(t){this._active=!0;let e;t?e=t.id:(e=this._scheduled,this._scheduled=void 0);let{actions:i}=this,r;t=t||i.shift();do if(r=t.execute(t.state,t.delay))break;while((t=i[0])&&t.id===e&&i.shift());if(this._active=!1,r){for(;(t=i[0])&&t.id===e&&i.shift();)t.unsubscribe();throw r}}};var Wl=new Gl($l);var ur=new ae(n=>n.complete());function ql(n){return n&&se(n.schedule)}function Ym(n){return n[n.length-1]}function Ql(n){return se(Ym(n))?n.pop():void 0}function An(n){return ql(Ym(n))?n.pop():void 0}function Bv(n,t){return typeof Ym(n)=="number"?n.pop():t}function jv(n,t,e,i){function r(o){return o instanceof e?o:new e(function(s){s(o)})}return new(e||(e=Promise))(function(o,s){function a(d){try{c(i.next(d))}catch(m){s(m)}}function l(d){try{c(i.throw(d))}catch(m){s(m)}}function c(d){d.done?o(d.value):r(d.value).then(a,l)}c((i=i.apply(n,t||[])).next())})}function Vv(n){var t=typeof Symbol=="function"&&Symbol.iterator,e=t&&n[t],i=0;if(e)return e.call(n);if(n&&typeof n.length=="number")return{next:function(){return n&&i>=n.length&&(n=void 0),{value:n&&n[i++],done:!n}}};throw new TypeError(t?"Object is not iterable.":"Symbol.iterator is not defined.")}function mr(n){return this instanceof mr?(this.v=n,this):new mr(n)}function zv(n,t,e){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var i=e.apply(n,t||[]),r,o=[];return r=Object.create((typeof AsyncIterator=="function"?AsyncIterator:Object).prototype),a("next"),a("throw"),a("return",s),r[Symbol.asyncIterator]=function(){return this},r;function s(h){return function(p){return Promise.resolve(p).then(h,m)}}function a(h,p){i[h]&&(r[h]=function(b){return new Promise(function(C,x){o.push([h,b,C,x])>1||l(h,b)})},p&&(r[h]=p(r[h])))}function l(h,p){try{c(i[h](p))}catch(b){f(o[0][3],b)}}function c(h){h.value instanceof mr?Promise.resolve(h.value.v).then(d,m):f(o[0][2],h)}function d(h){l("next",h)}function m(h){l("throw",h)}function f(h,p){h(p),o.shift(),o.length&&l(o[0][0],o[0][1])}}function Hv(n){if(!Symbol.asyncIterator)throw new TypeError("Symbol.asyncIterator is not defined.");var t=n[Symbol.asyncIterator],e;return t?t.call(n):(n=typeof Vv=="function"?Vv(n):n[Symbol.iterator](),e={},i("next"),i("throw"),i("return"),e[Symbol.asyncIterator]=function(){return this},e);function i(o){e[o]=n[o]&&function(s){return new Promise(function(a,l){s=n[o](s),r(a,l,s.done,s.value)})}}function r(o,s,a,l){Promise.resolve(l).then(function(c){o({value:c,done:a})},s)}}var Kl=n=>n&&typeof n.length=="number"&&typeof n!="function";function Yl(n){return se(n?.then)}function Zl(n){return se(n[ao])}function Xl(n){return Symbol.asyncIterator&&se(n?.[Symbol.asyncIterator])}function Jl(n){return new TypeError(`You provided ${n!==null&&typeof n=="object"?"an invalid object":`'${n}'`} where a stream was expected. You can provide an Observable, Promise, ReadableStream, Array, AsyncIterable, or Iterable.`)}function Bk(){return typeof Symbol!="function"||!Symbol.iterator?"@@iterator":Symbol.iterator}var ec=Bk();function tc(n){return se(n?.[ec])}function nc(n){return zv(this,arguments,function*(){let e=n.getReader();try{for(;;){let{value:i,done:r}=yield mr(e.read());if(r)return yield mr(void 0);yield yield mr(i)}}finally{e.releaseLock()}})}function ic(n){return se(n?.getReader)}function Re(n){if(n instanceof ae)return n;if(n!=null){if(Zl(n))return Vk(n);if(Kl(n))return jk(n);if(Yl(n))return zk(n);if(Xl(n))return Uv(n);if(tc(n))return Hk(n);if(ic(n))return Uk(n)}throw Jl(n)}function Vk(n){return new ae(t=>{let e=n[ao]();if(se(e.subscribe))return e.subscribe(t);throw new TypeError("Provided object does not correctly implement Symbol.observable")})}function jk(n){return new ae(t=>{for(let e=0;e<n.length&&!t.closed;e++)t.next(n[e]);t.complete()})}function zk(n){return new ae(t=>{n.then(e=>{t.closed||(t.next(e),t.complete())},e=>t.error(e)).then(null,Ll)})}function Hk(n){return new ae(t=>{for(let e of n)if(t.next(e),t.closed)return;t.complete()})}function Uv(n){return new ae(t=>{$k(n,t).catch(e=>t.error(e))})}function Uk(n){return Uv(nc(n))}function $k(n,t){var e,i,r,o;return jv(this,void 0,void 0,function*(){try{for(e=Hv(n);i=yield e.next(),!i.done;){let s=i.value;if(t.next(s),t.closed)return}}catch(s){r={error:s}}finally{try{i&&!i.done&&(o=e.return)&&(yield o.call(e))}finally{if(r)throw r.error}}t.complete()})}function Tt(n,t,e,i=0,r=!1){let o=t.schedule(function(){e(),r?n.add(this.schedule(null,i)):this.unsubscribe()},i);if(n.add(o),!r)return o}function rc(n,t=0){return he((e,i)=>{e.subscribe(ue(i,r=>Tt(i,n,()=>i.next(r),t),()=>Tt(i,n,()=>i.complete(),t),r=>Tt(i,n,()=>i.error(r),t)))})}function oc(n,t=0){return he((e,i)=>{i.add(n.schedule(()=>e.subscribe(i),t))})}function $v(n,t){return Re(n).pipe(oc(t),rc(t))}function Gv(n,t){return Re(n).pipe(oc(t),rc(t))}function Wv(n,t){return new ae(e=>{let i=0;return t.schedule(function(){i===n.length?e.complete():(e.next(n[i++]),e.closed||this.schedule())})})}function qv(n,t){return new ae(e=>{let i;return Tt(e,t,()=>{i=n[ec](),Tt(e,t,()=>{let r,o;try{({value:r,done:o}=i.next())}catch(s){e.error(s);return}o?e.complete():e.next(r)},0,!0)}),()=>se(i?.return)&&i.return()})}function sc(n,t){if(!n)throw new Error("Iterable cannot be null");return new ae(e=>{Tt(e,t,()=>{let i=n[Symbol.asyncIterator]();Tt(e,t,()=>{i.next().then(r=>{r.done?e.complete():e.next(r.value)})},0,!0)})})}function Qv(n,t){return sc(nc(n),t)}function Kv(n,t){if(n!=null){if(Zl(n))return $v(n,t);if(Kl(n))return Wv(n,t);if(Yl(n))return Gv(n,t);if(Xl(n))return sc(n,t);if(tc(n))return qv(n,t);if(ic(n))return Qv(n,t)}throw Jl(n)}function Yt(n,t){return t?Kv(n,t):Re(n)}function $e(...n){let t=An(n);return Yt(n,t)}function Zm(n,t){let e=se(n)?n:()=>n,i=r=>r.error(e());return new ae(t?r=>t.schedule(i,0,r):i)}function ac(n){return!!n&&(n instanceof ae||se(n.lift)&&se(n.subscribe))}function Yv(n){return n instanceof Date&&!isNaN(n)}function ye(n,t){return he((e,i)=>{let r=0;e.subscribe(ue(i,o=>{i.next(n.call(t,o,r++))}))})}var{isArray:Gk}=Array;function Wk(n,t){return Gk(t)?n(...t):n(t)}function lc(n){return ye(t=>Wk(n,t))}var{isArray:qk}=Array,{getPrototypeOf:Qk,prototype:Kk,keys:Yk}=Object;function cc(n){if(n.length===1){let t=n[0];if(qk(t))return{args:t,keys:null};if(Zk(t)){let e=Yk(t);return{args:e.map(i=>t[i]),keys:e}}}return{args:n,keys:null}}function Zk(n){return n&&typeof n=="object"&&Qk(n)===Kk}function dc(n,t){return n.reduce((e,i,r)=>(e[i]=t[r],e),{})}function hn(...n){let t=An(n),e=Ql(n),{args:i,keys:r}=cc(n);if(i.length===0)return Yt([],t);let o=new ae(Xk(i,t,r?s=>dc(r,s):mn));return e?o.pipe(lc(e)):o}function Xk(n,t,e=mn){return i=>{Zv(t,()=>{let{length:r}=n,o=new Array(r),s=r,a=r;for(let l=0;l<r;l++)Zv(t,()=>{let c=Yt(n[l],t),d=!1;c.subscribe(ue(i,m=>{o[l]=m,d||(d=!0,a--),a||i.next(e(o.slice()))},()=>{--s||i.complete()}))},i)},i)}}function Zv(n,t,e){n?Tt(e,n,t):t()}function Xv(n,t,e,i,r,o,s,a){let l=[],c=0,d=0,m=!1,f=()=>{m&&!l.length&&!c&&t.complete()},h=b=>c<i?p(b):l.push(b),p=b=>{o&&t.next(b),c++;let C=!1;Re(e(b,d++)).subscribe(ue(t,x=>{r?.(x),o?h(x):t.next(x)},()=>{C=!0},void 0,()=>{if(C)try{for(c--;l.length&&c<i;){let x=l.shift();s?Tt(t,s,()=>p(x)):p(x)}f()}catch(x){t.error(x)}}))};return n.subscribe(ue(t,h,()=>{m=!0,f()})),()=>{a?.()}}function uo(n,t,e=1/0){return se(t)?uo((i,r)=>ye((o,s)=>t(i,o,r,s))(Re(n(i,r))),e):(typeof t=="number"&&(e=t),he((i,r)=>Xv(i,r,n,e)))}function uc(n=1/0){return uo(mn,n)}function Jv(){return uc(1)}function mo(...n){return Jv()(Yt(n,An(n)))}function hr(n){return new ae(t=>{Re(n()).subscribe(t)})}function Is(...n){let t=Ql(n),{args:e,keys:i}=cc(n),r=new ae(o=>{let{length:s}=e;if(!s){o.complete();return}let a=new Array(s),l=s,c=s;for(let d=0;d<s;d++){let m=!1;Re(e[d]).subscribe(ue(o,f=>{m||(m=!0,c--),a[d]=f},()=>l--,void 0,()=>{(!l||!m)&&(c||o.next(i?dc(i,a):a),o.complete())}))}});return t?r.pipe(lc(t)):r}function eb(n=0,t,e=Lv){let i=-1;return t!=null&&(ql(t)?e=t:i=t),new ae(r=>{let o=Yv(n)?+n-e.now():n;o<0&&(o=0);let s=0;return e.schedule(function(){r.closed||(r.next(s++),0<=i?this.schedule(void 0,i):r.complete())},o)})}function Zt(...n){let t=An(n),e=Bv(n,1/0),i=n;return i.length?i.length===1?Re(i[0]):uc(e)(Yt(i,t)):ur}function ze(n,t){return he((e,i)=>{let r=0;e.subscribe(ue(i,o=>n.call(t,o,r++)&&i.next(o)))})}function tb(n){return he((t,e)=>{let i=!1,r=null,o=null,s=!1,a=()=>{if(o?.unsubscribe(),o=null,i){i=!1;let c=r;r=null,e.next(c)}s&&e.complete()},l=()=>{o=null,s&&e.complete()};t.subscribe(ue(e,c=>{i=!0,r=c,o||Re(n(c)).subscribe(o=ue(e,a,l))},()=>{s=!0,(!i||!o||o.closed)&&e.complete()}))})}function ho(n,t=Ss){return tb(()=>eb(n,t))}function mc(n){return he((t,e)=>{let i=null,r=!1,o;i=t.subscribe(ue(e,void 0,void 0,s=>{o=Re(n(s,mc(n)(t))),i?(i.unsubscribe(),i=null,o.subscribe(e)):r=!0})),r&&(i.unsubscribe(),i=null,o.subscribe(e))})}function Xm(n,t){return se(t)?uo(n,t,1):uo(n,1)}function Ms(n,t=Ss){return he((e,i)=>{let r=null,o=null,s=null,a=()=>{if(r){r.unsubscribe(),r=null;let c=o;o=null,i.next(c)}};function l(){let c=s+n,d=t.now();if(d<c){r=this.schedule(void 0,c-d),i.add(r);return}a()}e.subscribe(ue(i,c=>{o=c,s=t.now(),r||(r=t.schedule(l,n),i.add(r))},()=>{a(),i.complete()},void 0,()=>{o=r=null}))})}function Vt(n){return n<=0?()=>ur:he((t,e)=>{let i=0;t.subscribe(ue(e,r=>{++i<=n&&(e.next(r),n<=i&&e.complete())}))})}function hc(n,t=mn){return n=n??Jk,he((e,i)=>{let r,o=!0;e.subscribe(ue(i,s=>{let a=t(s);(o||!n(r,a))&&(o=!1,r=a,i.next(s))}))})}function Jk(n,t){return n===t}function fo(n){return he((t,e)=>{try{t.subscribe(e)}finally{e.add(n)}})}function fc(){return he((n,t)=>{let e,i=!1;n.subscribe(ue(t,r=>{let o=e;e=r,i&&t.next([o,r]),i=!0}))})}function Ts(n={}){let{connector:t=()=>new k,resetOnError:e=!0,resetOnComplete:i=!0,resetOnRefCountZero:r=!0}=n;return o=>{let s,a,l,c=0,d=!1,m=!1,f=()=>{a?.unsubscribe(),a=void 0},h=()=>{f(),s=l=void 0,d=m=!1},p=()=>{let b=s;h(),b?.unsubscribe()};return he((b,C)=>{c++,!m&&!d&&f();let x=l=l??t();C.add(()=>{c--,c===0&&!m&&!d&&(a=Jm(p,r))}),x.subscribe(C),!s&&c>0&&(s=new Kn({next:ee=>x.next(ee),error:ee=>{m=!0,f(),a=Jm(h,e,ee),x.error(ee)},complete:()=>{d=!0,f(),a=Jm(h,i),x.complete()}}),Re(b).subscribe(s))})(o)}}function Jm(n,t,...e){if(t===!0){n();return}if(t===!1)return;let i=new Kn({next:()=>{i.unsubscribe(),n()}});return Re(t(...e)).subscribe(i)}function pc(n,t,e){let i,r=!1;return n&&typeof n=="object"?{bufferSize:i=1/0,windowTime:t=1/0,refCount:r=!1,scheduler:e}=n:i=n??1/0,Ts({connector:()=>new Kt(i,t,e),resetOnError:!0,resetOnComplete:!1,resetOnRefCountZero:r})}function As(n){return ze((t,e)=>n<=e)}function jt(...n){let t=An(n);return he((e,i)=>{(t?mo(n,e,t):mo(n,e)).subscribe(i)})}function fr(n,t){return he((e,i)=>{let r=null,o=0,s=!1,a=()=>s&&!r&&i.complete();e.subscribe(ue(i,l=>{r?.unsubscribe();let c=0,d=o++;Re(n(l,d)).subscribe(r=ue(i,m=>i.next(t?t(l,m,d,c++):m),()=>{r=null,a()}))},()=>{s=!0,a()}))})}function Ce(n){return he((t,e)=>{Re(n).subscribe(ue(e,()=>e.complete(),Cs)),!e.closed&&t.subscribe(e)})}function eh(n,t=!1){return he((e,i)=>{let r=0;e.subscribe(ue(i,o=>{let s=n(o,r++);(s||t)&&i.next(o),!s&&i.complete()}))})}function pr(n,t,e){let i=se(n)||t||e?{next:n,error:t,complete:e}:n;return i?he((r,o)=>{var s;(s=i.subscribe)===null||s===void 0||s.call(i);let a=!0;r.subscribe(ue(o,l=>{var c;(c=i.next)===null||c===void 0||c.call(i,l),o.next(l)},()=>{var l;a=!1,(l=i.complete)===null||l===void 0||l.call(i),o.complete()},l=>{var c;a=!1,(c=i.error)===null||c===void 0||c.call(i,l),o.error(l)},()=>{var l,c;a&&((l=i.unsubscribe)===null||l===void 0||l.call(i)),(c=i.finalize)===null||c===void 0||c.call(i)}))}):mn}var th;function gc(){return th}function Rn(n){let t=th;return th=n,t}var nb=Symbol("NotFound");function po(n){return n===nb||n?.name==="\u0275NotFound"}function ib(n){let t=K(null);try{return n()}finally{K(t)}}var Dc="https://angular.dev/best-practices/security#preventing-cross-site-scripting-xss",D=class extends Error{code;constructor(t,e){super(_o(t,e)),this.code=t}};function eS(n){return`NG0${Math.abs(n)}`}function _o(n,t){return`${eS(n)}${t?": "+t:""}`}var Rt=globalThis;function xe(n){for(let t in n)if(n[t]===xe)return t;throw Error("")}function lb(n,t){for(let e in t)t.hasOwnProperty(e)&&!n.hasOwnProperty(e)&&(n[e]=t[e])}function Bs(n){if(typeof n=="string")return n;if(Array.isArray(n))return`[${n.map(Bs).join(", ")}]`;if(n==null)return""+n;let t=n.overriddenName||n.name;if(t)return`${t}`;let e=n.toString();if(e==null)return""+e;let i=e.indexOf(`
`);return i>=0?e.slice(0,i):e}function xc(n,t){return n?t?`${n} ${t}`:n:t||""}var tS=xe({__forward_ref__:xe});function bt(n){return n.__forward_ref__=bt,n}function mt(n){return fh(n)?n():n}function fh(n){return typeof n=="function"&&n.hasOwnProperty(tS)&&n.__forward_ref__===bt}function w(n){return{token:n.token,providedIn:n.providedIn||null,factory:n.factory,value:void 0}}function S(n){return{providers:n.providers||[],imports:n.imports||[]}}function Ec(n){return nS(n,kc)}function nS(n,t){return n.hasOwnProperty(t)&&n[t]||null}function iS(n){let t=n?.[kc]??null;return t||null}function ih(n){return n&&n.hasOwnProperty(vc)?n[vc]:null}var kc=xe({\u0275prov:xe}),vc=xe({\u0275inj:xe}),v=class{_desc;ngMetadataName="InjectionToken";\u0275prov;constructor(t,e){this._desc=t,this.\u0275prov=void 0,typeof e=="number"?this.__NG_ELEMENT_ID__=e:e!==void 0&&(this.\u0275prov=w({token:this,providedIn:e.providedIn||"root",factory:e.factory}))}get multi(){return this}toString(){return`InjectionToken ${this._desc}`}};function ph(n){return n&&!!n.\u0275providers}var Vs=xe({\u0275cmp:xe}),js=xe({\u0275dir:xe}),gh=xe({\u0275pipe:xe}),_h=xe({\u0275mod:xe}),Os=xe({\u0275fac:xe}),wr=xe({__NG_ELEMENT_ID__:xe}),rb=xe({__NG_ENV_ID__:xe});function cb(n){return Sc(n,"@NgModule"),n[_h]||null}function Ii(n){return Sc(n,"@Component"),n[Vs]||null}function vh(n){return Sc(n,"@Directive"),n[js]||null}function db(n){return Sc(n,"@Pipe"),n[gh]||null}function Sc(n,t){if(n==null)throw new D(-919,!1)}function bh(n){return typeof n=="string"?n:n==null?"":String(n)}var ub=xe({ngErrorCode:xe}),rS=xe({ngErrorMessage:xe}),oS=xe({ngTokenPath:xe});function yh(n,t){return mb("",-200,t)}function Ic(n,t){throw new D(-201,!1)}function mb(n,t,e){let i=new D(t,n);return i[ub]=t,i[rS]=n,e&&(i[oS]=e),i}function sS(n){return n[ub]}var rh;function hb(){return rh}function zt(n){let t=rh;return rh=n,t}function wh(n,t,e){let i=Ec(n);if(i&&i.providedIn=="root")return i.value===void 0?i.value=i.factory():i.value;if(e&8)return null;if(t!==void 0)return t;Ic(n,"")}var aS={},gr=aS,lS="__NG_DI_FLAG__",oh=class{injector;constructor(t){this.injector=t}retrieve(t,e){let i=_r(e)||0;try{return this.injector.get(t,i&8?null:gr,i)}catch(r){if(po(r))return r;throw r}}};function cS(n,t=0){let e=gc();if(e===void 0)throw new D(-203,!1);if(e===null)return wh(n,void 0,t);{let i=dS(t),r=e.retrieve(n,i);if(po(r)){if(i.optional)return null;throw r}return r}}function B(n,t=0){return(hb()||cS)(mt(n),t)}function u(n,t){return B(n,_r(t))}function _r(n){return typeof n>"u"||typeof n=="number"?n:0|(n.optional&&8)|(n.host&&1)|(n.self&&2)|(n.skipSelf&&4)}function dS(n){return{optional:!!(n&8),host:!!(n&1),self:!!(n&2),skipSelf:!!(n&4)}}function sh(n){let t=[];for(let e=0;e<n.length;e++){let i=mt(n[e]);if(Array.isArray(i)){if(i.length===0)throw new D(900,!1);let r,o=0;for(let s=0;s<i.length;s++){let a=i[s],l=uS(a);typeof l=="number"?l===-1?r=a.token:o|=l:r=a}t.push(B(r,o))}else t.push(B(i))}return t}function uS(n){return n[lS]}function vr(n,t){let e=n.hasOwnProperty(Os);return e?n[Os]:null}function fb(n,t,e){if(n.length!==t.length)return!1;for(let i=0;i<n.length;i++){let r=n[i],o=t[i];if(e&&(r=e(r),o=e(o)),o!==r)return!1}return!0}function pb(n){return n.flat(Number.POSITIVE_INFINITY)}function Mc(n,t){n.forEach(e=>Array.isArray(e)?Mc(e,t):t(e))}function Ch(n,t,e){t>=n.length?n.push(e):n.splice(t,0,e)}function zs(n,t){return t>=n.length-1?n.pop():n.splice(t,1)[0]}function gb(n,t){let e=[];for(let i=0;i<n;i++)e.push(t);return e}function _b(n,t,e,i){let r=n.length;if(r==t)n.push(e,i);else if(r===1)n.push(i,n[0]),n[0]=e;else{for(r--,n.push(n[r-1],n[r]);r>t;){let o=r-2;n[r]=n[o],r--}n[t]=e,n[t+1]=i}}function Tc(n,t,e){let i=vo(n,t);return i>=0?n[i|1]=e:(i=~i,_b(n,i,t,e)),i}function Ac(n,t){let e=vo(n,t);if(e>=0)return n[e|1]}function vo(n,t){return mS(n,t,1)}function mS(n,t,e){let i=0,r=n.length>>e;for(;r!==i;){let o=i+(r-i>>1),s=n[o<<e];if(t===s)return o<<e;s>t?r=o:i=o+1}return~(r<<e)}var Mi={},vt=[],bo=new v(""),Dh=new v("",-1),xh=new v(""),Ns=class{get(t,e=gr){if(e===gr){let r=mb("",-201);throw r.name="\u0275NotFound",r}return e}};function Hs(n){return{\u0275providers:n}}function vb(...n){return{\u0275providers:Eh(!0,n),\u0275fromNgModule:!0}}function Eh(n,...t){let e=[],i=new Set,r,o=s=>{e.push(s)};return Mc(t,s=>{let a=s;bc(a,o,[],i)&&(r||=[],r.push(a))}),r!==void 0&&bb(r,o),e}function bb(n,t){for(let e=0;e<n.length;e++){let{ngModule:i,providers:r}=n[e];kh(r,o=>{t(o,i)})}}function bc(n,t,e,i){if(n=mt(n),!n)return!1;let r=null,o=ih(n),s=!o&&Ii(n);if(!o&&!s){let l=n.ngModule;if(o=ih(l),o)r=l;else return!1}else{if(s&&!s.standalone)return!1;r=n}let a=i.has(r);if(s){if(a)return!1;if(i.add(r),s.dependencies){let l=typeof s.dependencies=="function"?s.dependencies():s.dependencies;for(let c of l)bc(c,t,e,i)}}else if(o){if(o.imports!=null&&!a){i.add(r);let c;Mc(o.imports,d=>{bc(d,t,e,i)&&(c||=[],c.push(d))}),c!==void 0&&bb(c,t)}if(!a){let c=vr(r)||(()=>new r);t({provide:r,useFactory:c,deps:vt},r),t({provide:xh,useValue:r,multi:!0},r),t({provide:bo,useValue:()=>B(r),multi:!0},r)}let l=o.providers;if(l!=null&&!a){let c=n;kh(l,d=>{t(d,c)})}}else return!1;return r!==n&&n.providers!==void 0}function kh(n,t){for(let e of n)ph(e)&&(e=e.\u0275providers),Array.isArray(e)?kh(e,t):t(e)}var hS=xe({provide:String,useValue:xe});function yb(n){return n!==null&&typeof n=="object"&&hS in n}function fS(n){return!!(n&&n.useExisting)}function pS(n){return!!(n&&n.useFactory)}function br(n){return typeof n=="function"}function wb(n){return!!n.useClass}var Us=new v(""),_c={},ob={},nh;function yo(){return nh===void 0&&(nh=new Ns),nh}var Ze=class{},yr=class extends Ze{parent;source;scopes;records=new Map;_ngOnDestroyHooks=new Set;_onDestroyHooks=[];get destroyed(){return this._destroyed}_destroyed=!1;injectorDefTypes;constructor(t,e,i,r){super(),this.parent=e,this.source=i,this.scopes=r,lh(t,s=>this.processProvider(s)),this.records.set(Dh,go(void 0,this)),r.has("environment")&&this.records.set(Ze,go(void 0,this));let o=this.records.get(Us);o!=null&&typeof o.value=="string"&&this.scopes.add(o.value),this.injectorDefTypes=new Set(this.get(xh,vt,{self:!0}))}retrieve(t,e){let i=_r(e)||0;try{return this.get(t,gr,i)}catch(r){if(po(r))return r;throw r}}destroy(){Rs(this),this._destroyed=!0;let t=K(null);try{for(let i of this._ngOnDestroyHooks)i.ngOnDestroy();let e=this._onDestroyHooks;this._onDestroyHooks=[];for(let i of e)i()}finally{this.records.clear(),this._ngOnDestroyHooks.clear(),this.injectorDefTypes.clear(),K(t)}}onDestroy(t){return Rs(this),this._onDestroyHooks.push(t),()=>this.removeOnDestroy(t)}runInContext(t){Rs(this);let e=Rn(this),i=zt(void 0),r;try{return t()}finally{Rn(e),zt(i)}}get(t,e=gr,i){if(Rs(this),t.hasOwnProperty(rb))return t[rb](this);let r=_r(i),o,s=Rn(this),a=zt(void 0);try{if(!(r&4)){let c=this.records.get(t);if(c===void 0){let d=yS(t)&&Ec(t);d&&this.injectableDefInScope(d)?c=go(ah(t),_c):c=null,this.records.set(t,c)}if(c!=null)return this.hydrate(t,c,r)}let l=r&2?yo():this.parent;return e=r&8&&e===gr?null:e,l.get(t,e)}catch(l){let c=sS(l);throw c===-200||c===-201?new D(c,null):l}finally{zt(a),Rn(s)}}resolveInjectorInitializers(){let t=K(null),e=Rn(this),i=zt(void 0),r;try{let o=this.get(bo,vt,{self:!0});for(let s of o)s()}finally{Rn(e),zt(i),K(t)}}toString(){return"R3Injector[...]"}processProvider(t){t=mt(t);let e=br(t)?t:mt(t&&t.provide),i=_S(t);if(!br(t)&&t.multi===!0){let r=this.records.get(e);r||(r=go(void 0,_c,!0),r.factory=()=>sh(r.multi),this.records.set(e,r)),e=t,r.multi.push(t)}this.records.set(e,i)}hydrate(t,e,i){let r=K(null);try{if(e.value===ob)throw yh("");return e.value===_c&&(e.value=ob,e.value=e.factory(void 0,i)),typeof e.value=="object"&&e.value&&bS(e.value)&&this._ngOnDestroyHooks.add(e.value),e.value}finally{K(r)}}injectableDefInScope(t){if(!t.providedIn)return!1;let e=mt(t.providedIn);return typeof e=="string"?e==="any"||this.scopes.has(e):this.injectorDefTypes.has(e)}removeOnDestroy(t){let e=this._onDestroyHooks.indexOf(t);e!==-1&&this._onDestroyHooks.splice(e,1)}};function ah(n){let t=Ec(n),e=t!==null?t.factory:vr(n);if(e!==null)return e;if(n instanceof v)throw new D(-204,!1);if(n instanceof Function)return gS(n);throw new D(-204,!1)}function gS(n){if(n.length>0)throw new D(-204,!1);let e=iS(n);return e!==null?()=>e.factory(n):()=>new n}function _S(n){if(yb(n))return go(void 0,n.useValue);{let t=Sh(n);return go(t,_c)}}function Sh(n,t,e){let i;if(br(n)){let r=mt(n);return vr(r)||ah(r)}else if(yb(n))i=()=>mt(n.useValue);else if(pS(n))i=()=>n.useFactory(...sh(n.deps||[]));else if(fS(n))i=(r,o)=>B(mt(n.useExisting),o!==void 0&&o&8?8:void 0);else{let r=mt(n&&(n.useClass||n.provide));if(vS(n))i=()=>new r(...sh(n.deps));else return vr(r)||ah(r)}return i}function Rs(n){if(n.destroyed)throw new D(-205,!1)}function go(n,t,e=!1){return{factory:n,value:t,multi:e?[]:void 0}}function vS(n){return!!n.deps}function bS(n){return n!==null&&typeof n=="object"&&typeof n.ngOnDestroy=="function"}function yS(n){return typeof n=="function"||typeof n=="object"&&n.ngMetadataName==="InjectionToken"}function lh(n,t){for(let e of n)Array.isArray(e)?lh(e,t):e&&ph(e)?lh(e.\u0275providers,t):t(e)}function wo(n,t){let e;n instanceof yr?(Rs(n),e=n):e=new oh(n);let i,r=Rn(e),o=zt(void 0);try{return t()}finally{Rn(r),zt(o)}}function Ih(){return hb()!==void 0||gc()!=null}var fn=0,Y=1,te=2,rt=3,Xt=4,xt=5,Cr=6,Co=7,Xe=8,Zn=9,Nn=10,Se=11,Do=12,Mh=13,Dr=14,Et=15,Ti=16,xr=17,Fn=18,Xn=19,Th=20,Yn=21,Rc=22,ki=23,Ht=24,Er=25,Ai=26,Ge=27,Cb=1,Ah=6,Ri=7,$s=8,kr=9,We=10;function Jn(n){return Array.isArray(n)&&typeof n[Cb]=="object"}function pn(n){return Array.isArray(n)&&n[Cb]===!0}function Rh(n){return(n.flags&4)!==0}function Pn(n){return n.componentOffset>-1}function xo(n){return(n.flags&1)===1}function Ln(n){return!!n.template}function Eo(n){return(n[te]&512)!==0}function Sr(n){return(n[te]&256)===256}var ct=(function(n){return n[n.NONE=0]="NONE",n[n.HTML=1]="HTML",n[n.STYLE=2]="STYLE",n[n.SCRIPT=3]="SCRIPT",n[n.URL=4]="URL",n[n.RESOURCE_URL=5]="RESOURCE_URL",n[n.ATTRIBUTE_NO_BINDING=6]="ATTRIBUTE_NO_BINDING",n})(ct||{});var Oh="svg",Db="math";function Jt(n){for(;Array.isArray(n);)n=n[fn];return n}function Nh(n,t){return Jt(t[n])}function en(n,t){return Jt(t[n.index])}function Oc(n,t){return n.data[t]}function xb(n,t){return n[t]}function tn(n,t){let e=t[n];return Jn(e)?e:e[fn]}function Eb(n){return(n[te]&4)===4}function Nc(n){return(n[te]&128)===128}function kb(n){return pn(n[rt])}function nn(n,t){return t==null?null:n[t]}function Fh(n){n[xr]=0}function Ph(n){n[te]&1024||(n[te]|=1024,Nc(n)&&Ir(n))}function Sb(n,t){for(;n>0;)t=t[Dr],n--;return t}function Gs(n){return!!(n[te]&9216||n[Ht]?.dirty)}function Fc(n){n[Nn].changeDetectionScheduler?.notify(8),n[te]&64&&(n[te]|=1024),Gs(n)&&Ir(n)}function Ir(n){n[Nn].changeDetectionScheduler?.notify(0);let t=Si(n);for(;t!==null&&!(t[te]&8192||(t[te]|=8192,!Nc(t)));)t=Si(t)}function Lh(n,t){if(Sr(n))throw new D(911,!1);n[Yn]===null&&(n[Yn]=[]),n[Yn].push(t)}function Ib(n,t){if(n[Yn]===null)return;let e=n[Yn].indexOf(t);e!==-1&&n[Yn].splice(e,1)}function Si(n){let t=n[rt];return pn(t)?t[rt]:t}function Bh(n){return n[Co]??=[]}function Vh(n){return n.cleanup??=[]}function Mb(n,t,e,i){let r=Bh(t);r.push(e),n.firstCreatePass&&Vh(n).push(i,r.length-1)}var le={lFrame:zb(null),bindingsEnabled:!0,skipHydrationRootTNode:null};var ch=!1;function Tb(){return le.lFrame.elementDepthCount}function Ab(){le.lFrame.elementDepthCount++}function jh(){le.lFrame.elementDepthCount--}function Pc(){return le.bindingsEnabled}function zh(){return le.skipHydrationRootTNode!==null}function Hh(n){return le.skipHydrationRootTNode===n}function Uh(){le.skipHydrationRootTNode=null}function ne(){return le.lFrame.lView}function Ne(){return le.lFrame.tView}function He(n){return le.lFrame.contextLView=n,n[Xe]}function Ue(n){return le.lFrame.contextLView=null,n}function ot(){let n=$h();for(;n!==null&&n.type===64;)n=n.parent;return n}function $h(){return le.lFrame.currentTNode}function Rb(){let n=le.lFrame,t=n.currentTNode;return n.isParent?t:t.parent}function ko(n,t){let e=le.lFrame;e.currentTNode=n,e.isParent=t}function Gh(){return le.lFrame.isParent}function Wh(){le.lFrame.isParent=!1}function Ob(){return le.lFrame.contextLView}function qh(){return ch}function Fs(n){let t=ch;return ch=n,t}function Nb(){let n=le.lFrame,t=n.bindingRootIndex;return t===-1&&(t=n.bindingRootIndex=n.tView.bindingStartIndex),t}function Fb(n){return le.lFrame.bindingIndex=n}function ei(){return le.lFrame.bindingIndex++}function Qh(n){let t=le.lFrame,e=t.bindingIndex;return t.bindingIndex=t.bindingIndex+n,e}function Pb(){return le.lFrame.inI18n}function Lb(n,t){let e=le.lFrame;e.bindingIndex=e.bindingRootIndex=n,Lc(t)}function Bb(){return le.lFrame.currentDirectiveIndex}function Lc(n){le.lFrame.currentDirectiveIndex=n}function Vb(n){let t=le.lFrame.currentDirectiveIndex;return t===-1?null:n[t]}function Bc(){return le.lFrame.currentQueryIndex}function Ws(n){le.lFrame.currentQueryIndex=n}function wS(n){let t=n[Y];return t.type===2?t.declTNode:t.type===1?n[xt]:null}function Kh(n,t,e){if(e&4){let r=t,o=n;for(;r=r.parent,r===null&&!(e&1);)if(r=wS(o),r===null||(o=o[Dr],r.type&10))break;if(r===null)return!1;t=r,n=o}let i=le.lFrame=jb();return i.currentTNode=t,i.lView=n,!0}function Vc(n){let t=jb(),e=n[Y];le.lFrame=t,t.currentTNode=e.firstChild,t.lView=n,t.tView=e,t.contextLView=n,t.bindingIndex=e.bindingStartIndex,t.inI18n=!1}function jb(){let n=le.lFrame,t=n===null?null:n.child;return t===null?zb(n):t}function zb(n){let t={currentTNode:null,isParent:!0,lView:null,tView:null,selectedIndex:-1,contextLView:null,elementDepthCount:0,currentNamespace:null,currentDirectiveIndex:-1,bindingRootIndex:-1,bindingIndex:-1,currentQueryIndex:0,parent:n,child:null,inI18n:!1};return n!==null&&(n.child=t),t}function Hb(){let n=le.lFrame;return le.lFrame=n.parent,n.currentTNode=null,n.lView=null,n}var Yh=Hb;function jc(){let n=Hb();n.isParent=!0,n.tView=null,n.selectedIndex=-1,n.contextLView=null,n.elementDepthCount=0,n.currentDirectiveIndex=-1,n.currentNamespace=null,n.bindingRootIndex=-1,n.bindingIndex=-1,n.currentQueryIndex=0}function Ub(n){return(le.lFrame.contextLView=Sb(n,le.lFrame.contextLView))[Xe]}function ti(){return le.lFrame.selectedIndex}function Oi(n){le.lFrame.selectedIndex=n}function So(){let n=le.lFrame;return Oc(n.tView,n.selectedIndex)}function yt(){le.lFrame.currentNamespace=Oh}function ni(){CS()}function CS(){le.lFrame.currentNamespace=null}function Zh(){return le.lFrame.currentNamespace}var $b=!0;function zc(){return $b}function qs(n){$b=n}function dh(n,t=null,e=null,i){let r=Xh(n,t,e,i);return r.resolveInjectorInitializers(),r}function Xh(n,t=null,e=null,i,r=new Set){let o=[e||vt,vb(n)],s;return new yr(o,t||yo(),s||null,r)}var $=class n{static THROW_IF_NOT_FOUND=gr;static NULL=new Ns;static create(t,e){if(Array.isArray(t))return dh({name:""},e,t,"");{let i=t.name??"";return dh({name:i},t.parent,t.providers,i)}}static \u0275prov=w({token:n,providedIn:"any",factory:()=>B(Dh)});static __NG_ELEMENT_ID__=-1},U=new v(""),gn=(()=>{class n{static __NG_ELEMENT_ID__=DS;static __NG_ENV_ID__=e=>e}return n})(),yc=class extends gn{_lView;constructor(t){super(),this._lView=t}get destroyed(){return Sr(this._lView)}onDestroy(t){let e=this._lView;return Lh(e,t),()=>Ib(e,t)}};function DS(){return new yc(ne())}var Gb=!1,Wb=new v(""),Mr=(()=>{class n{taskId=0;pendingTasks=new Set;destroyed=!1;pendingTask=new Bt(!1);debugTaskTracker=u(Wb,{optional:!0});get hasPendingTasks(){return this.destroyed?!1:this.pendingTask.value}get hasPendingTasksObservable(){return this.destroyed?new ae(e=>{e.next(!1),e.complete()}):this.pendingTask}add(){!this.hasPendingTasks&&!this.destroyed&&this.pendingTask.next(!0);let e=this.taskId++;return this.pendingTasks.add(e),this.debugTaskTracker?.add(e),e}has(e){return this.pendingTasks.has(e)}remove(e){this.pendingTasks.delete(e),this.debugTaskTracker?.remove(e),this.pendingTasks.size===0&&this.hasPendingTasks&&this.pendingTask.next(!1)}ngOnDestroy(){this.pendingTasks.clear(),this.hasPendingTasks&&this.pendingTask.next(!1),this.destroyed=!0,this.pendingTask.unsubscribe()}static \u0275prov=w({token:n,providedIn:"root",factory:()=>new n})}return n})(),uh=class extends k{__isAsync;destroyRef=void 0;pendingTasks=void 0;constructor(t=!1){super(),this.__isAsync=t,Ih()&&(this.destroyRef=u(gn,{optional:!0})??void 0,this.pendingTasks=u(Mr,{optional:!0})??void 0)}emit(t){let e=K(null);try{super.next(t)}finally{K(e)}}subscribe(t,e,i){let r=t,o=e||(()=>null),s=i;if(t&&typeof t=="object"){let l=t;r=l.next?.bind(l),o=l.error?.bind(l),s=l.complete?.bind(l)}this.__isAsync&&(o=this.wrapInTimeout(o),r&&(r=this.wrapInTimeout(r)),s&&(s=this.wrapInTimeout(s)));let a=super.subscribe({next:r,error:o,complete:s});return t instanceof ge&&t.add(a),a}wrapInTimeout(t){return e=>{let i=this.pendingTasks?.add();setTimeout(()=>{try{t(e)}finally{i!==void 0&&this.pendingTasks?.remove(i)}})}}},z=uh;function wc(...n){}function Jh(n){let t,e;function i(){n=wc;try{e!==void 0&&typeof cancelAnimationFrame=="function"&&cancelAnimationFrame(e),t!==void 0&&clearTimeout(t)}catch(r){}}return t=setTimeout(()=>{n(),i()}),typeof requestAnimationFrame=="function"&&(e=requestAnimationFrame(()=>{n(),i()})),()=>i()}function qb(n){return queueMicrotask(()=>n()),()=>{n=wc}}var ef="isAngularZone",Ps=ef+"_ID",xS=0,F=class n{hasPendingMacrotasks=!1;hasPendingMicrotasks=!1;isStable=!0;onUnstable=new z(!1);onMicrotaskEmpty=new z(!1);onStable=new z(!1);onError=new z(!1);constructor(t){let{enableLongStackTrace:e=!1,shouldCoalesceEventChangeDetection:i=!1,shouldCoalesceRunChangeDetection:r=!1,scheduleInRootZone:o=Gb}=t;if(typeof Zone>"u")throw new D(908,!1);Zone.assertZonePatched();let s=this;s._nesting=0,s._outer=s._inner=Zone.current,Zone.TaskTrackingZoneSpec&&(s._inner=s._inner.fork(new Zone.TaskTrackingZoneSpec)),e&&Zone.longStackTraceZoneSpec&&(s._inner=s._inner.fork(Zone.longStackTraceZoneSpec)),s.shouldCoalesceEventChangeDetection=!r&&i,s.shouldCoalesceRunChangeDetection=r,s.callbackScheduled=!1,s.scheduleInRootZone=o,SS(s)}static isInAngularZone(){return typeof Zone<"u"&&Zone.current.get(ef)===!0}static assertInAngularZone(){if(!n.isInAngularZone())throw new D(909,!1)}static assertNotInAngularZone(){if(n.isInAngularZone())throw new D(909,!1)}run(t,e,i){return this._inner.run(t,e,i)}runTask(t,e,i,r){let o=this._inner,s=o.scheduleEventTask("NgZoneEvent: "+r,t,ES,wc,wc);try{return o.runTask(s,e,i)}finally{o.cancelTask(s)}}runGuarded(t,e,i){return this._inner.runGuarded(t,e,i)}runOutsideAngular(t){return this._outer.run(t)}},ES={};function tf(n){if(n._nesting==0&&!n.hasPendingMicrotasks&&!n.isStable)try{n._nesting++,n.onMicrotaskEmpty.emit(null)}finally{if(n._nesting--,!n.hasPendingMicrotasks)try{n.runOutsideAngular(()=>n.onStable.emit(null))}finally{n.isStable=!0}}}function kS(n){if(n.isCheckStableRunning||n.callbackScheduled)return;n.callbackScheduled=!0;function t(){Jh(()=>{n.callbackScheduled=!1,mh(n),n.isCheckStableRunning=!0,tf(n),n.isCheckStableRunning=!1})}n.scheduleInRootZone?Zone.root.run(()=>{t()}):n._outer.run(()=>{t()}),mh(n)}function SS(n){let t=()=>{kS(n)},e=xS++;n._inner=n._inner.fork({name:"angular",properties:{[ef]:!0,[Ps]:e,[Ps+e]:!0},onInvokeTask:(i,r,o,s,a,l)=>{if(IS(l))return i.invokeTask(o,s,a,l);try{return sb(n),i.invokeTask(o,s,a,l)}finally{(n.shouldCoalesceEventChangeDetection&&s.type==="eventTask"||n.shouldCoalesceRunChangeDetection)&&t(),ab(n)}},onInvoke:(i,r,o,s,a,l,c)=>{try{return sb(n),i.invoke(o,s,a,l,c)}finally{n.shouldCoalesceRunChangeDetection&&!n.callbackScheduled&&!MS(l)&&t(),ab(n)}},onHasTask:(i,r,o,s)=>{i.hasTask(o,s),r===o&&(s.change=="microTask"?(n._hasPendingMicrotasks=s.microTask,mh(n),tf(n)):s.change=="macroTask"&&(n.hasPendingMacrotasks=s.macroTask))},onHandleError:(i,r,o,s)=>(i.handleError(o,s),n.runOutsideAngular(()=>n.onError.emit(s)),!1)})}function mh(n){n._hasPendingMicrotasks||(n.shouldCoalesceEventChangeDetection||n.shouldCoalesceRunChangeDetection)&&n.callbackScheduled===!0?n.hasPendingMicrotasks=!0:n.hasPendingMicrotasks=!1}function sb(n){n._nesting++,n.isStable&&(n.isStable=!1,n.onUnstable.emit(null))}function ab(n){n._nesting--,tf(n)}var Ls=class{hasPendingMicrotasks=!1;hasPendingMacrotasks=!1;isStable=!0;onUnstable=new z;onMicrotaskEmpty=new z;onStable=new z;onError=new z;run(t,e,i){return t.apply(e,i)}runGuarded(t,e,i){return t.apply(e,i)}runOutsideAngular(t){return t()}runTask(t,e,i,r){return t.apply(e,i)}};function IS(n){return Qb(n,"__ignore_ng_zone__")}function MS(n){return Qb(n,"__scheduler_tick__")}function Qb(n,t){return!Array.isArray(n)||n.length!==1?!1:n[0]?.data?.[t]===!0}var At=class{_console=console;handleError(t){this._console.error("ERROR",t)}},Ni=new v("",{factory:()=>{let n=u(F),t=u(Ze),e;return i=>{n.runOutsideAngular(()=>{t.destroyed&&!e?setTimeout(()=>{throw i}):(e??=t.get(At),e.handleError(i))})}}}),Kb={provide:bo,useValue:()=>{let n=u(At,{optional:!0})},multi:!0};function Oe(n,t){let[e,i,r]=Lm(n,t?.equal),o=e,s=o[lt];return o.set=i,o.update=r,o.asReadonly=Yb.bind(o),o}function Yb(){let n=this[lt];if(n.readonlyFn===void 0){let t=()=>this();t[lt]=n,n.readonlyFn=t}return n.readonlyFn}var Io=(()=>{class n{view;node;constructor(e,i){this.view=e,this.node=i}static __NG_ELEMENT_ID__=TS}return n})();function TS(){return new Io(ne(),ot())}var On=class{},Qs=new v("",{factory:()=>!0});var nf=new v(""),Ks=(()=>{class n{internalPendingTasks=u(Mr);scheduler=u(On);errorHandler=u(Ni);add(){let e=this.internalPendingTasks.add();return()=>{this.internalPendingTasks.has(e)&&(this.scheduler.notify(11),this.internalPendingTasks.remove(e))}}run(e){let i=this.add();e().catch(this.errorHandler).finally(i)}static \u0275prov=w({token:n,providedIn:"root",factory:()=>new n})}return n})(),Hc=(()=>{class n{static \u0275prov=w({token:n,providedIn:"root",factory:()=>new hh})}return n})(),hh=class{dirtyEffectCount=0;queues=new Map;add(t){this.enqueue(t),this.schedule(t)}schedule(t){t.dirty&&this.dirtyEffectCount++}remove(t){let e=t.zone,i=this.queues.get(e);i.has(t)&&(i.delete(t),t.dirty&&this.dirtyEffectCount--)}enqueue(t){let e=t.zone;this.queues.has(e)||this.queues.set(e,new Set);let i=this.queues.get(e);i.has(t)||i.add(t)}flush(){for(;this.dirtyEffectCount>0;){let t=!1;for(let[e,i]of this.queues)e===null?t||=this.flushQueue(i):t||=e.run(()=>this.flushQueue(i));t||(this.dirtyEffectCount=0)}}flushQueue(t){let e=!1;for(let i of t)i.dirty&&(this.dirtyEffectCount--,e=!0,i.run());return e}},Cc=class{[lt];constructor(t){this[lt]=t}destroy(){this[lt].destroy()}};function Fi(n,t){let e=t?.injector??u($),i=t?.manualCleanup!==!0?e.get(gn):null,r,o=e.get(Io,null,{optional:!0}),s=e.get(On);return o!==null?(r=OS(o.view,s,n),i instanceof yc&&i._lView===o.view&&(i=null)):r=NS(n,e.get(Hc),s),r.injector=e,i!==null&&(r.onDestroyFns=[i.onDestroy(()=>r.destroy())]),new Cc(r)}var Zb=be(M({},Vm),{cleanupFns:void 0,zone:null,onDestroyFns:null,run(){let n=Fs(!1);try{jm(this)}finally{Fs(n)}},cleanup(){if(!this.cleanupFns?.length)return;let n=K(null);try{for(;this.cleanupFns.length;)this.cleanupFns.pop()()}finally{this.cleanupFns=[],K(n)}}}),AS=be(M({},Zb),{consumerMarkedDirty(){this.scheduler.schedule(this),this.notifier.notify(12)},destroy(){if(Di(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.scheduler.remove(this)}}),RS=be(M({},Zb),{consumerMarkedDirty(){this.view[te]|=8192,Ir(this.view),this.notifier.notify(13)},destroy(){if(Di(this),this.onDestroyFns!==null)for(let n of this.onDestroyFns)n();this.cleanup(),this.view[ki]?.delete(this)}});function OS(n,t,e){let i=Object.create(RS);return i.view=n,i.zone=typeof Zone<"u"?Zone.current:null,i.notifier=t,i.fn=Xb(i,e),n[ki]??=new Set,n[ki].add(i),i.consumerMarkedDirty(i),i}function NS(n,t,e){let i=Object.create(AS);return i.fn=Xb(i,n),i.scheduler=t,i.notifier=e,i.zone=typeof Zone<"u"?Zone.current:null,i.scheduler.add(i),i.notifier.notify(12),i}function Xb(n,t){return()=>{t(e=>(n.cleanupFns??=[]).push(e))}}function sa(n){return{toString:n}.toString()}function Ay(n){let t=Rt.ng;if(t&&t.\u0275compilerFacade)return t.\u0275compilerFacade;throw new Error("JIT compiler unavailable")}function US(n){return typeof n=="function"}function Ry(n,t,e,i){t!==null?t.applyValueToInputSignal(t,i):n[e]=i}var Xc=class{previousValue;currentValue;firstChange;constructor(t,e,i){this.previousValue=t,this.currentValue=e,this.firstChange=i}isFirstChange(){return this.firstChange}},Le=(()=>{let n=()=>Oy;return n.ngInherit=!0,n})();function Oy(n){return n.type.prototype.ngOnChanges&&(n.setInput=GS),$S}function $S(){let n=Fy(this),t=n?.current;if(t){let e=n.previous;if(e===Mi)n.previous=t;else for(let i in t)e[i]=t[i];n.current=null,this.ngOnChanges(t)}}function GS(n,t,e,i,r){let o=this.declaredInputs[i],s=Fy(n)||WS(n,{previous:Mi,current:null}),a=s.current||(s.current={}),l=s.previous,c=l[o];a[o]=new Xc(c&&c.currentValue,e,l===Mi),Ry(n,t,r,e)}var Ny="__ngSimpleChanges__";function Fy(n){return n[Ny]||null}function WS(n,t){return n[Ny]=t}var Jb=[];var Me=function(n,t=null,e){for(let i=0;i<Jb.length;i++){let r=Jb[i];r(n,t,e)}},De=(function(n){return n[n.TemplateCreateStart=0]="TemplateCreateStart",n[n.TemplateCreateEnd=1]="TemplateCreateEnd",n[n.TemplateUpdateStart=2]="TemplateUpdateStart",n[n.TemplateUpdateEnd=3]="TemplateUpdateEnd",n[n.LifecycleHookStart=4]="LifecycleHookStart",n[n.LifecycleHookEnd=5]="LifecycleHookEnd",n[n.OutputStart=6]="OutputStart",n[n.OutputEnd=7]="OutputEnd",n[n.BootstrapApplicationStart=8]="BootstrapApplicationStart",n[n.BootstrapApplicationEnd=9]="BootstrapApplicationEnd",n[n.BootstrapComponentStart=10]="BootstrapComponentStart",n[n.BootstrapComponentEnd=11]="BootstrapComponentEnd",n[n.ChangeDetectionStart=12]="ChangeDetectionStart",n[n.ChangeDetectionEnd=13]="ChangeDetectionEnd",n[n.ChangeDetectionSyncStart=14]="ChangeDetectionSyncStart",n[n.ChangeDetectionSyncEnd=15]="ChangeDetectionSyncEnd",n[n.AfterRenderHooksStart=16]="AfterRenderHooksStart",n[n.AfterRenderHooksEnd=17]="AfterRenderHooksEnd",n[n.ComponentStart=18]="ComponentStart",n[n.ComponentEnd=19]="ComponentEnd",n[n.DeferBlockStateStart=20]="DeferBlockStateStart",n[n.DeferBlockStateEnd=21]="DeferBlockStateEnd",n[n.DynamicComponentStart=22]="DynamicComponentStart",n[n.DynamicComponentEnd=23]="DynamicComponentEnd",n[n.HostBindingsUpdateStart=24]="HostBindingsUpdateStart",n[n.HostBindingsUpdateEnd=25]="HostBindingsUpdateEnd",n})(De||{});function qS(n,t,e){let{ngOnChanges:i,ngOnInit:r,ngDoCheck:o}=t.type.prototype;if(i){let s=Oy(t);(e.preOrderHooks??=[]).push(n,s),(e.preOrderCheckHooks??=[]).push(n,s)}r&&(e.preOrderHooks??=[]).push(0-n,r),o&&((e.preOrderHooks??=[]).push(n,o),(e.preOrderCheckHooks??=[]).push(n,o))}function Py(n,t){for(let e=t.directiveStart,i=t.directiveEnd;e<i;e++){let o=n.data[e].type.prototype,{ngAfterContentInit:s,ngAfterContentChecked:a,ngAfterViewInit:l,ngAfterViewChecked:c,ngOnDestroy:d}=o;s&&(n.contentHooks??=[]).push(-e,s),a&&((n.contentHooks??=[]).push(e,a),(n.contentCheckHooks??=[]).push(e,a)),l&&(n.viewHooks??=[]).push(-e,l),c&&((n.viewHooks??=[]).push(e,c),(n.viewCheckHooks??=[]).push(e,c)),d!=null&&(n.destroyHooks??=[]).push(e,d)}}function qc(n,t,e){Ly(n,t,3,e)}function Qc(n,t,e,i){(n[te]&3)===e&&Ly(n,t,e,i)}function rf(n,t){let e=n[te];(e&3)===t&&(e&=16383,e+=1,n[te]=e)}function Ly(n,t,e,i){let r=i!==void 0?n[xr]&65535:0,o=i??-1,s=t.length-1,a=0;for(let l=r;l<s;l++)if(typeof t[l+1]=="number"){if(a=t[l],i!=null&&a>=i)break}else t[l]<0&&(n[xr]+=65536),(a<o||o==-1)&&(QS(n,e,t,l),n[xr]=(n[xr]&4294901760)+l+2),l++}function ey(n,t){Me(De.LifecycleHookStart,n,t);let e=K(null);try{t.call(n)}finally{K(e),Me(De.LifecycleHookEnd,n,t)}}function QS(n,t,e,i){let r=e[i]<0,o=e[i+1],s=r?-e[i]:e[i],a=n[s];r?n[te]>>14<n[xr]>>16&&(n[te]&3)===t&&(n[te]+=16384,ey(a,o)):ey(a,o)}var To=-1,Ar=class{factory;name;injectImpl;resolving=!1;canSeeViewProviders;multi;componentProviders;index;providerFactory;constructor(t,e,i,r){this.factory=t,this.name=r,this.canSeeViewProviders=e,this.injectImpl=i}};function KS(n){return(n.flags&8)!==0}function YS(n){return(n.flags&16)!==0}function ZS(n,t,e){let i=0;for(;i<e.length;){let r=e[i];if(typeof r=="number"){if(r!==0)break;i++;let o=e[i++],s=e[i++],a=e[i++];n.setAttribute(t,s,a,o)}else{let o=r,s=e[++i];XS(o)?n.setProperty(t,o,s):n.setAttribute(t,o,s),i++}}return i}function By(n){return n===3||n===4||n===6}function XS(n){return n.charCodeAt(0)===64}function Ao(n,t){if(!(t===null||t.length===0))if(n===null||n.length===0)n=t.slice();else{let e=-1;for(let i=0;i<t.length;i++){let r=t[i];typeof r=="number"?e=r:e===0||(e===-1||e===2?ty(n,e,r,null,t[++i]):ty(n,e,r,null,null))}}return n}function ty(n,t,e,i,r){let o=0,s=n.length;if(t===-1)s=-1;else for(;o<n.length;){let a=n[o++];if(typeof a=="number"){if(a===t){s=-1;break}else if(a>t){s=o-1;break}}}for(;o<n.length;){let a=n[o];if(typeof a=="number")break;if(a===e){r!==null&&(n[o+1]=r);return}o++,r!==null&&o++}s!==-1&&(n.splice(s,0,t),o=s+1),n.splice(o++,0,e),r!==null&&n.splice(o++,0,r)}function Vy(n){return n!==To}function Jc(n){return n&32767}function JS(n){return n>>16}function ed(n,t){let e=JS(n),i=t;for(;e>0;)i=i[Dr],e--;return i}var ff=!0;function ny(n){let t=ff;return ff=n,t}var eI=256,jy=eI-1,zy=5,tI=0,Bn={};function nI(n,t,e){let i;typeof e=="string"?i=e.charCodeAt(0)||0:e.hasOwnProperty(wr)&&(i=e[wr]),i==null&&(i=e[wr]=tI++);let r=i&jy,o=1<<r;t.data[n+(r>>zy)]|=o}function td(n,t){let e=Hy(n,t);if(e!==-1)return e;let i=t[Y];i.firstCreatePass&&(n.injectorIndex=t.length,of(i.data,n),of(t,null),of(i.blueprint,null));let r=Zf(n,t),o=n.injectorIndex;if(Vy(r)){let s=Jc(r),a=ed(r,t),l=a[Y].data;for(let c=0;c<8;c++)t[o+c]=a[s+c]|l[s+c]}return t[o+8]=r,o}function of(n,t){n.push(0,0,0,0,0,0,0,0,t)}function Hy(n,t){return n.injectorIndex===-1||n.parent&&n.parent.injectorIndex===n.injectorIndex||t[n.injectorIndex+8]===null?-1:n.injectorIndex}function Zf(n,t){if(n.parent&&n.parent.injectorIndex!==-1)return n.parent.injectorIndex;let e=0,i=null,r=t;for(;r!==null;){if(i=qy(r),i===null)return To;if(e++,r=r[Dr],i.injectorIndex!==-1)return i.injectorIndex|e<<16}return To}function pf(n,t,e){nI(n,t,e)}function iI(n,t){if(t==="class")return n.classes;if(t==="style")return n.styles;let e=n.attrs;if(e){let i=e.length,r=0;for(;r<i;){let o=e[r];if(By(o))break;if(o===0)r=r+2;else if(typeof o=="number")for(r++;r<i&&typeof e[r]=="string";)r++;else{if(o===t)return e[r+1];r=r+2}}}return null}function Uy(n,t,e){if(e&8||n!==void 0)return n;Ic(t,"NodeInjector")}function $y(n,t,e,i){if(e&8&&i===void 0&&(i=null),(e&3)===0){let r=n[Zn],o=zt(void 0);try{return r?r.get(t,i,e&8):wh(t,i,e&8)}finally{zt(o)}}return Uy(i,t,e)}function Gy(n,t,e,i=0,r){if(n!==null){if(t[te]&2048&&!(i&2)){let s=aI(n,t,e,i,Bn);if(s!==Bn)return s}let o=Wy(n,t,e,i,Bn);if(o!==Bn)return o}return $y(t,e,i,r)}function Wy(n,t,e,i,r){let o=oI(e);if(typeof o=="function"){if(!Kh(t,n,i))return i&1?Uy(r,e,i):$y(t,e,i,r);try{let s;if(s=o(i),s==null&&!(i&8))Ic(e);else return s}finally{Yh()}}else if(typeof o=="number"){let s=null,a=Hy(n,t),l=To,c=i&1?t[Et][xt]:null;for((a===-1||i&4)&&(l=a===-1?Zf(n,t):t[a+8],l===To||!ry(i,!1)?a=-1:(s=t[Y],a=Jc(l),t=ed(l,t)));a!==-1;){let d=t[Y];if(iy(o,a,d.data)){let m=rI(a,t,e,s,i,c);if(m!==Bn)return m}l=t[a+8],l!==To&&ry(i,t[Y].data[a+8]===c)&&iy(o,a,t)?(s=d,a=Jc(l),t=ed(l,t)):a=-1}}return r}function rI(n,t,e,i,r,o){let s=t[Y],a=s.data[n+8],l=i==null?Pn(a)&&ff:i!=s&&(a.type&3)!==0,c=r&1&&o===a,d=Kc(a,s,e,l,c);return d!==null?Js(t,s,d,a,r):Bn}function Kc(n,t,e,i,r){let o=n.providerIndexes,s=t.data,a=o&1048575,l=n.directiveStart,c=n.directiveEnd,d=o>>20,m=i?a:a+d,f=r?a+d:c;for(let h=m;h<f;h++){let p=s[h];if(h<l&&e===p||h>=l&&p.type===e)return h}if(r){let h=s[l];if(h&&Ln(h)&&h.type===e)return l}return null}function Js(n,t,e,i,r){let o=n[e],s=t.data;if(o instanceof Ar){let a=o;if(a.resolving)throw yh("");let l=ny(a.canSeeViewProviders);a.resolving=!0;let c=s[e].type||s[e],d,m=a.injectImpl?zt(a.injectImpl):null,f=Kh(n,i,0);try{o=n[e]=a.factory(void 0,r,s,n,i),t.firstCreatePass&&e>=i.directiveStart&&qS(e,s[e],t)}finally{m!==null&&zt(m),ny(l),a.resolving=!1,Yh()}}return o}function oI(n){if(typeof n=="string")return n.charCodeAt(0)||0;let t=n.hasOwnProperty(wr)?n[wr]:void 0;return typeof t=="number"?t>=0?t&jy:sI:t}function iy(n,t,e){let i=1<<n;return!!(e[t+(n>>zy)]&i)}function ry(n,t){return!(n&2)&&!(n&1&&t)}var Tr=class{_tNode;_lView;constructor(t,e){this._tNode=t,this._lView=e}get(t,e,i){return Gy(this._tNode,this._lView,t,_r(i),e)}};function sI(){return new Tr(ot(),ne())}function Be(n){return sa(()=>{let t=n.prototype.constructor,e=t[Os]||gf(t),i=Object.prototype,r=Object.getPrototypeOf(n.prototype).constructor;for(;r&&r!==i;){let o=r[Os]||gf(r);if(o&&o!==e)return o;r=Object.getPrototypeOf(r)}return o=>new o})}function gf(n){return fh(n)?()=>{let t=gf(mt(n));return t&&t()}:vr(n)}function aI(n,t,e,i,r){let o=n,s=t;for(;o!==null&&s!==null&&s[te]&2048&&!Eo(s);){let a=Wy(o,s,e,i|2,Bn);if(a!==Bn)return a;let l=o.parent;if(!l){let c=s[Th];if(c){let d=c.get(e,Bn,i&-5);if(d!==Bn)return d}l=qy(s),s=s[Dr]}o=l}return r}function qy(n){let t=n[Y],e=t.type;return e===2?t.declTNode:e===1?n[xt]:null}function Xf(n){return iI(ot(),n)}function lI(){return Po(ot(),ne())}function Po(n,t){return new P(en(n,t))}var P=(()=>{class n{nativeElement;constructor(e){this.nativeElement=e}static __NG_ELEMENT_ID__=lI}return n})();function Qy(n){return n instanceof P?n.nativeElement:n}function cI(){return this._results[Symbol.iterator]()}var Pi=class{_emitDistinctChangesOnly;dirty=!0;_onDirty=void 0;_results=[];_changesDetected=!1;_changes=void 0;length=0;first=void 0;last=void 0;get changes(){return this._changes??=new k}constructor(t=!1){this._emitDistinctChangesOnly=t}get(t){return this._results[t]}map(t){return this._results.map(t)}filter(t){return this._results.filter(t)}find(t){return this._results.find(t)}reduce(t,e){return this._results.reduce(t,e)}forEach(t){this._results.forEach(t)}some(t){return this._results.some(t)}toArray(){return this._results.slice()}toString(){return this._results.toString()}reset(t,e){this.dirty=!1;let i=pb(t);(this._changesDetected=!fb(this._results,i,e))&&(this._results=i,this.length=i.length,this.last=i[this.length-1],this.first=i[0])}notifyOnChanges(){this._changes!==void 0&&(this._changesDetected||!this._emitDistinctChangesOnly)&&this._changes.next(this)}onDirty(t){this._onDirty=t}setDirty(){this.dirty=!0,this._onDirty?.()}destroy(){this._changes!==void 0&&(this._changes.complete(),this._changes.unsubscribe())}[Symbol.iterator]=cI};function Ky(n){return(n.flags&128)===128}var Jf=(function(n){return n[n.OnPush=0]="OnPush",n[n.Eager=1]="Eager",n[n.Default=1]="Default",n})(Jf||{}),Yy=new Map,dI=0;function uI(){return dI++}function mI(n){Yy.set(n[Xn],n)}function _f(n){Yy.delete(n[Xn])}var oy="__ngContext__";function Ro(n,t){Jn(t)?(n[oy]=t[Xn],mI(t)):n[oy]=t}function Zy(n){return Jy(n[Do])}function Xy(n){return Jy(n[Xt])}function Jy(n){for(;n!==null&&!pn(n);)n=n[Xt];return n}var hI;function ep(n){hI=n}var Bi=new v("",{factory:()=>fI}),fI="ng";var hd=new v(""),Nr=new v("",{providedIn:"platform",factory:()=>"unknown"}),Vi=new v(""),Fr=new v("",{factory:()=>u(U).body?.querySelector("[ngCspNonce]")?.getAttribute("ngCspNonce")||null});var e0="r";var t0="di";var n0=!1,i0=new v("",{factory:()=>n0});var sy=new WeakMap;function pI(n,t){if(n==null||typeof n!="object")return;let e=sy.get(n);e||(e=new WeakSet,sy.set(n,e)),e.add(t)}var gI=(n,t,e,i)=>{};function _I(n,t,e,i){gI(n,t,e,i)}function fd(n){return(n.flags&32)===32}var vI=()=>null;function r0(n,t,e=!1){return vI(n,t,e)}function o0(n,t){let e=n.contentQueries;if(e!==null){let i=K(null);try{for(let r=0;r<e.length;r+=2){let o=e[r],s=e[r+1];if(s!==-1){let a=n.data[s];Ws(o),a.contentQueries(2,t[s],s)}}}finally{K(i)}}}function vf(n,t,e){Ws(0);let i=K(null);try{t(n,e)}finally{K(i)}}function tp(n,t,e){if(Rh(t)){let i=K(null);try{let r=t.directiveStart,o=t.directiveEnd;for(let s=r;s<o;s++){let a=n.data[s];if(a.contentQueries){let l=e[s];a.contentQueries(1,l,s)}}}finally{K(i)}}}var bn=(function(n){return n[n.Emulated=0]="Emulated",n[n.None=2]="None",n[n.ShadowDom=3]="ShadowDom",n[n.ExperimentalIsolatedShadowDom=4]="ExperimentalIsolatedShadowDom",n})(bn||{});var Uc;function bI(){if(Uc===void 0&&(Uc=null,Rt.trustedTypes))try{Uc=Rt.trustedTypes.createPolicy("angular",{createHTML:n=>n,createScript:n=>n,createScriptURL:n=>n})}catch(n){}return Uc}function pd(n){return bI()?.createHTML(n)||n}var ii=class{changingThisBreaksApplicationSecurity;constructor(t){this.changingThisBreaksApplicationSecurity=t}toString(){return`SafeValue must use [property]=binding: ${this.changingThisBreaksApplicationSecurity} (see ${Dc})`}},bf=class extends ii{getTypeName(){return"HTML"}},yf=class extends ii{getTypeName(){return"Style"}},wf=class extends ii{getTypeName(){return"Script"}},Cf=class extends ii{getTypeName(){return"URL"}},Df=class extends ii{getTypeName(){return"ResourceURL"}};function ri(n){return n instanceof ii?n.changingThisBreaksApplicationSecurity:n}function Pr(n,t){let e=s0(n);if(e!=null&&e!==t){if(e==="ResourceURL"&&t==="URL")return!0;throw new Error(`Required a safe ${t}, got a ${e} (see ${Dc})`)}return e===t}function s0(n){return n instanceof ii&&n.getTypeName()||null}function np(n){return new bf(n)}function ip(n){return new yf(n)}function rp(n){return new wf(n)}function op(n){return new Cf(n)}function sp(n){return new Df(n)}function yI(n){let t=new Ef(n);return wI()?new xf(t):t}var xf=class{inertDocumentHelper;constructor(t){this.inertDocumentHelper=t}getInertBodyElement(t){t="<body><remove></remove>"+t;try{let e=new window.DOMParser().parseFromString(pd(t),"text/html").body;return e===null?this.inertDocumentHelper.getInertBodyElement(t):(e.firstChild?.remove(),e)}catch(e){return null}}},Ef=class{defaultDoc;inertDocument;constructor(t){this.defaultDoc=t,this.inertDocument=this.defaultDoc.implementation.createHTMLDocument("sanitization-inert")}getInertBodyElement(t){let e=this.inertDocument.createElement("template");return e.innerHTML=pd(t),e}};function wI(){try{return!!new window.DOMParser().parseFromString(pd(""),"text/html")}catch(n){return!1}}var CI=/^(?!javascript:)(?:[a-z0-9+.-]+:|[^&:\/?#]*(?:[\/?#]|$))/i;function gd(n){return n=String(n),n.match(CI)?n:"unsafe:"+n}function oi(n){let t={};for(let e of n.split(","))t[e]=!0;return t}function aa(...n){let t={};for(let e of n)for(let i in e)e.hasOwnProperty(i)&&(t[i]=!0);return t}var a0=oi("area,br,col,hr,img,wbr"),l0=oi("colgroup,dd,dt,li,p,tbody,td,tfoot,th,thead,tr"),c0=oi("rp,rt"),DI=aa(c0,l0),xI=aa(l0,oi("address,article,aside,blockquote,caption,center,del,details,dialog,dir,div,dl,figure,figcaption,footer,h1,h2,h3,h4,h5,h6,header,hgroup,hr,ins,main,map,menu,nav,ol,pre,section,summary,table,ul")),EI=aa(c0,oi("a,abbr,acronym,audio,b,bdi,bdo,big,br,cite,code,del,dfn,em,font,i,img,ins,kbd,label,map,mark,picture,q,ruby,rp,rt,s,samp,small,source,span,strike,strong,sub,sup,time,track,tt,u,var,video")),ay=aa(a0,xI,EI,DI),d0=oi("background,cite,href,itemtype,longdesc,poster,src,xlink:href"),kI=oi("abbr,accesskey,align,alt,autoplay,axis,bgcolor,border,cellpadding,cellspacing,class,clear,color,cols,colspan,compact,controls,coords,datetime,default,dir,download,face,headers,height,hidden,hreflang,hspace,ismap,itemscope,itemprop,kind,label,lang,language,loop,media,muted,nohref,nowrap,open,preload,rel,rev,role,rows,rowspan,rules,scope,scrolling,shape,size,sizes,span,srclang,srcset,start,summary,tabindex,target,title,translate,type,usemap,valign,value,vspace,width"),SI=oi("aria-activedescendant,aria-atomic,aria-autocomplete,aria-busy,aria-checked,aria-colcount,aria-colindex,aria-colspan,aria-controls,aria-current,aria-describedby,aria-details,aria-disabled,aria-dropeffect,aria-errormessage,aria-expanded,aria-flowto,aria-grabbed,aria-haspopup,aria-hidden,aria-invalid,aria-keyshortcuts,aria-label,aria-labelledby,aria-level,aria-live,aria-modal,aria-multiline,aria-multiselectable,aria-orientation,aria-owns,aria-placeholder,aria-posinset,aria-pressed,aria-readonly,aria-relevant,aria-required,aria-roledescription,aria-rowcount,aria-rowindex,aria-rowspan,aria-selected,aria-setsize,aria-sort,aria-valuemax,aria-valuemin,aria-valuenow,aria-valuetext"),II=aa(d0,kI,SI),MI=oi("script,style,template"),kf=class{sanitizedSomething=!1;buf=[];sanitizeChildren(t){let e=t.firstChild,i=!0,r=[];for(;e;){if(e.nodeType===Node.ELEMENT_NODE?i=this.startElement(e):e.nodeType===Node.TEXT_NODE?this.chars(e.nodeValue):this.sanitizedSomething=!0,i&&e.firstChild){r.push(e),e=RI(e);continue}for(;e;){e.nodeType===Node.ELEMENT_NODE&&this.endElement(e);let o=AI(e);if(o){e=o;break}e=r.pop()}}return this.buf.join("")}startElement(t){let e=ly(t).toLowerCase();if(!ay.hasOwnProperty(e))return this.sanitizedSomething=!0,!MI.hasOwnProperty(e);this.buf.push("<"),this.buf.push(e);let i=t.attributes;for(let r=0;r<i.length;r++){let o=i.item(r),s=o.name,a=s.toLowerCase();if(!II.hasOwnProperty(a)){this.sanitizedSomething=!0;continue}let l=o.value;d0[a]&&(l=gd(l)),this.buf.push(" ",s,'="',cy(l),'"')}return this.buf.push(">"),!0}endElement(t){let e=ly(t).toLowerCase();ay.hasOwnProperty(e)&&!a0.hasOwnProperty(e)&&(this.buf.push("</"),this.buf.push(e),this.buf.push(">"))}chars(t){this.buf.push(cy(t))}};function TI(n,t){return(n.compareDocumentPosition(t)&Node.DOCUMENT_POSITION_CONTAINED_BY)!==Node.DOCUMENT_POSITION_CONTAINED_BY}function AI(n){let t=n.nextSibling;if(t&&n!==t.previousSibling)throw u0(t);return t}function RI(n){let t=n.firstChild;if(t&&TI(n,t))throw u0(t);return t}function ly(n){let t=n.nodeName;return typeof t=="string"?t:"FORM"}function u0(n){return new Error(`Failed to sanitize html because the element is clobbered: ${n.outerHTML}`)}var OI=/[\uD800-\uDBFF][\uDC00-\uDFFF]/g,NI=/([^\#-~ |!])/g;function cy(n){return n.replace(/&/g,"&amp;").replace(OI,function(t){let e=t.charCodeAt(0),i=t.charCodeAt(1);return"&#"+((e-55296)*1024+(i-56320)+65536)+";"}).replace(NI,function(t){return"&#"+t.charCodeAt(0)+";"}).replace(/</g,"&lt;").replace(/>/g,"&gt;")}var $c;function ap(n,t){let e=null;try{$c=$c||yI(n);let i=t?String(t):"";e=$c.getInertBodyElement(i);let r=5,o=i;do{if(r===0)throw new Error("Failed to sanitize html because the input is unstable");r--,i=o,o=e.innerHTML,e=$c.getInertBodyElement(i)}while(i!==o);let a=new kf().sanitizeChildren(dy(e)||e);return pd(a)}finally{if(e){let i=dy(e)||e;for(;i.firstChild;)i.firstChild.remove()}}}function dy(n){return"content"in n&&FI(n)?n.content:null}function FI(n){return n.nodeType===Node.ELEMENT_NODE&&n.nodeName==="TEMPLATE"}var PI=/^>|^->|<!--|-->|--!>|<!-$/g,LI=/(<|>)/g,BI="\u200B$1\u200B";function VI(n){return n.replace(PI,t=>t.replace(LI,BI))}function jI(n,t){return n.createText(t)}function zI(n,t,e){n.setValue(t,e)}function HI(n,t){return n.createComment(VI(t))}function m0(n,t,e){return n.createElement(t,e)}function nd(n,t,e,i,r){n.insertBefore(t,e,i,r)}function h0(n,t,e){n.appendChild(t,e)}function uy(n,t,e,i,r){i!==null?nd(n,t,e,i,r):h0(n,t,e)}function f0(n,t,e,i){n.removeChild(null,t,e,i)}function UI(n,t,e){n.setAttribute(t,"style",e)}function $I(n,t,e){e===""?n.removeAttribute(t,"class"):n.setAttribute(t,"class",e)}function p0(n,t,e){let{mergedAttrs:i,classes:r,styles:o}=e;i!==null&&ZS(n,t,i),r!==null&&$I(n,t,r),o!==null&&UI(n,t,o)}function GI(n){return n instanceof Function?n():n}function WI(n,t,e){let i=n.length;for(;;){let r=n.indexOf(t,e);if(r===-1)return r;if(r===0||n.charCodeAt(r-1)<=32){let o=t.length;if(r+o===i||n.charCodeAt(r+o)<=32)return r}e=r+1}}var g0="ng-template";function qI(n,t,e,i){let r=0;if(i){for(;r<t.length&&typeof t[r]=="string";r+=2)if(t[r]==="class"&&WI(t[r+1].toLowerCase(),e,0)!==-1)return!0}else if(lp(n))return!1;if(r=t.indexOf(1,r),r>-1){let o;for(;++r<t.length&&typeof(o=t[r])=="string";)if(o.toLowerCase()===e)return!0}return!1}function lp(n){return n.type===4&&n.value!==g0}function QI(n,t,e){let i=n.type===4&&!e?g0:n.value;return t===i}function KI(n,t,e){let i=4,r=n.attrs,o=r!==null?XI(r):0,s=!1;for(let a=0;a<t.length;a++){let l=t[a];if(typeof l=="number"){if(!s&&!_n(i)&&!_n(l))return!1;if(s&&_n(l))continue;s=!1,i=l|i&1;continue}if(!s)if(i&4){if(i=2|i&1,l!==""&&!QI(n,l,e)||l===""&&t.length===1){if(_n(i))return!1;s=!0}}else if(i&8){if(r===null||!qI(n,r,l,e)){if(_n(i))return!1;s=!0}}else{let c=t[++a],d=YI(l,r,lp(n),e);if(d===-1){if(_n(i))return!1;s=!0;continue}if(c!==""){let m;if(d>o?m="":m=r[d+1].toLowerCase(),i&2&&c!==m){if(_n(i))return!1;s=!0}}}}return _n(i)||s}function _n(n){return(n&1)===0}function YI(n,t,e,i){if(t===null)return-1;let r=0;if(i||!e){let o=!1;for(;r<t.length;){let s=t[r];if(s===n)return r;if(s===3||s===6)o=!0;else if(s===1||s===2){let a=t[++r];for(;typeof a=="string";)a=t[++r];continue}else{if(s===4)break;if(s===0){r+=4;continue}}r+=o?1:2}return-1}else return JI(t,n)}function _0(n,t,e=!1){for(let i=0;i<t.length;i++)if(KI(n,t[i],e))return!0;return!1}function ZI(n){let t=n.attrs;if(t!=null){let e=t.indexOf(5);if((e&1)===0)return t[e+1]}return null}function XI(n){for(let t=0;t<n.length;t++){let e=n[t];if(By(e))return t}return n.length}function JI(n,t){let e=n.indexOf(4);if(e>-1)for(e++;e<n.length;){let i=n[e];if(typeof i=="number")return-1;if(i===t)return e;e++}return-1}function eM(n,t){e:for(let e=0;e<t.length;e++){let i=t[e];if(n.length===i.length){for(let r=0;r<n.length;r++)if(n[r]!==i[r])continue e;return!0}}return!1}function my(n,t){return n?":not("+t.trim()+")":t}function tM(n){let t=n[0],e=1,i=2,r="",o=!1;for(;e<n.length;){let s=n[e];if(typeof s=="string")if(i&2){let a=n[++e];r+="["+s+(a.length>0?'="'+a+'"':"")+"]"}else i&8?r+="."+s:i&4&&(r+=" "+s);else r!==""&&!_n(s)&&(t+=my(o,r),r=""),i=s,o=o||!_n(i);e++}return r!==""&&(t+=my(o,r)),t}function nM(n){return n.map(tM).join(",")}function iM(n){let t=[],e=[],i=1,r=2;for(;i<n.length;){let o=n[i];if(typeof o=="string")r===2?o!==""&&t.push(o,n[++i]):r===8&&e.push(o);else{if(!_n(r))break;r=o}i++}return e.length&&t.push(1,...e),t}var rn={};function cp(n,t,e,i,r,o,s,a,l,c,d){let m=Ge+i,f=m+r,h=rM(m,f),p=typeof c=="function"?c():c;return h[Y]={type:n,blueprint:h,template:e,queries:null,viewQuery:a,declTNode:t,data:h.slice().fill(null,m),bindingStartIndex:m,expandoStartIndex:f,hostBindingOpCodes:null,firstCreatePass:!0,firstUpdatePass:!0,staticViewQueries:!1,staticContentQueries:!1,preOrderHooks:null,preOrderCheckHooks:null,contentHooks:null,contentCheckHooks:null,viewHooks:null,viewCheckHooks:null,destroyHooks:null,cleanup:null,contentQueries:null,components:null,directiveRegistry:typeof o=="function"?o():o,pipeRegistry:typeof s=="function"?s():s,firstChild:null,schemas:l,consts:p,incompleteFirstPass:!1,ssrId:d}}function rM(n,t){let e=[];for(let i=0;i<t;i++)e.push(i<n?null:rn);return e}function oM(n){let t=n.tView;return t===null||t.incompleteFirstPass?n.tView=cp(1,null,n.template,n.decls,n.vars,n.directiveDefs,n.pipeDefs,n.viewQuery,n.schemas,n.consts,n.id):t}function dp(n,t,e,i,r,o,s,a,l,c,d){let m=t.blueprint.slice();return m[fn]=r,m[te]=i|4|128|8|64|1024,(c!==null||n&&n[te]&2048)&&(m[te]|=2048),Fh(m),m[rt]=m[Dr]=n,m[Xe]=e,m[Nn]=s||n&&n[Nn],m[Se]=a||n&&n[Se],m[Zn]=l||n&&n[Zn]||null,m[xt]=o,m[Xn]=uI(),m[Cr]=d,m[Th]=c,m[Et]=t.type==2?n[Et]:m,m}function sM(n,t,e){let i=en(t,n),r=oM(e),o=n[Nn].rendererFactory,s=up(n,dp(n,r,null,v0(e),i,t,null,o.createRenderer(i,e),null,null,null));return n[t.index]=s}function v0(n){let t=16;return n.signals?t=4096:n.onPush&&(t=64),t}function b0(n,t,e,i){if(e===0)return-1;let r=t.length;for(let o=0;o<e;o++)t.push(i),n.blueprint.push(i),n.data.push(null);return r}function up(n,t){return n[Do]?n[Mh][Xt]=t:n[Do]=t,n[Mh]=t,t}function y(n=1){y0(Ne(),ne(),ti()+n,!1)}function y0(n,t,e,i){if(!i)if((t[te]&3)===3){let o=n.preOrderCheckHooks;o!==null&&qc(t,o,e)}else{let o=n.preOrderHooks;o!==null&&Qc(t,o,0,e)}Oi(e)}var _d=(function(n){return n[n.None=0]="None",n[n.SignalBased=1]="SignalBased",n[n.HasDecoratorInputTransform=2]="HasDecoratorInputTransform",n})(_d||{});function Sf(n,t,e,i){let r=K(null);try{let[o,s,a]=n.inputs[e],l=null;(s&_d.SignalBased)!==0&&(l=t[o][lt]),l!==null&&l.transformFn!==void 0?i=l.transformFn(i):a!==null&&(i=a.call(t,i)),n.setInput!==null?n.setInput(t,l,i,e,o):Ry(t,l,o,i)}finally{K(r)}}var Vn=(function(n){return n[n.Important=1]="Important",n[n.DashCase=2]="DashCase",n})(Vn||{}),aM;function mp(n,t){return aM(n,t)}var XH=typeof document<"u"&&typeof document?.documentElement?.getAnimations=="function";var If=new WeakMap,Ys=new WeakSet;function lM(n,t){let e=If.get(n);if(!e||e.length===0)return;let i=t.parentNode,r=t.previousSibling;for(let o=e.length-1;o>=0;o--){let s=e[o],a=s.parentNode;s===t?(e.splice(o,1),Ys.add(s),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}}))):(r&&s===r||a&&i&&a!==i)&&(e.splice(o,1),s.dispatchEvent(new CustomEvent("animationend",{detail:{cancel:!0}})),s.parentNode?.removeChild(s))}}function cM(n,t){let e=If.get(n);e?e.includes(t)||e.push(t):If.set(n,[t])}var Rr=new Set,vd=(function(n){return n[n.CHANGE_DETECTION=0]="CHANGE_DETECTION",n[n.AFTER_NEXT_RENDER=1]="AFTER_NEXT_RENDER",n})(vd||{}),yn=new v(""),hy=new Set;function ji(n){hy.has(n)||(hy.add(n),performance?.mark?.("mark_feature_usage",{detail:{feature:n}}))}var bd=(()=>{class n{impl=null;execute(){this.impl?.execute()}static \u0275prov=w({token:n,providedIn:"root",factory:()=>new n})}return n})(),hp=[0,1,2,3],fp=(()=>{class n{ngZone=u(F);scheduler=u(On);errorHandler=u(At,{optional:!0});sequences=new Set;deferredRegistrations=new Set;executing=!1;constructor(){u(yn,{optional:!0})}execute(){let e=this.sequences.size>0;e&&Me(De.AfterRenderHooksStart),this.executing=!0;for(let i of hp)for(let r of this.sequences)if(!(r.erroredOrDestroyed||!r.hooks[i]))try{r.pipelinedValue=this.ngZone.runOutsideAngular(()=>this.maybeTrace(()=>{let o=r.hooks[i];return o(r.pipelinedValue)},r.snapshot))}catch(o){r.erroredOrDestroyed=!0,this.errorHandler?.handleError(o)}this.executing=!1;for(let i of this.sequences)i.afterRun(),i.once&&(this.sequences.delete(i),i.destroy());for(let i of this.deferredRegistrations)this.sequences.add(i);this.deferredRegistrations.size>0&&this.scheduler.notify(7),this.deferredRegistrations.clear(),e&&Me(De.AfterRenderHooksEnd)}register(e){let{view:i}=e;i!==void 0?((i[Er]??=[]).push(e),Ir(i),i[te]|=8192):this.executing?this.deferredRegistrations.add(e):this.addSequence(e)}addSequence(e){this.sequences.add(e),this.scheduler.notify(7)}unregister(e){this.executing&&this.sequences.has(e)?(e.erroredOrDestroyed=!0,e.pipelinedValue=void 0,e.once=!0):(this.sequences.delete(e),this.deferredRegistrations.delete(e))}maybeTrace(e,i){return i?i.run(vd.AFTER_NEXT_RENDER,e):e()}static \u0275prov=w({token:n,providedIn:"root",factory:()=>new n})}return n})(),ea=class{impl;hooks;view;once;snapshot;erroredOrDestroyed=!1;pipelinedValue=void 0;unregisterOnDestroy;constructor(t,e,i,r,o,s=null){this.impl=t,this.hooks=e,this.view=i,this.once=r,this.snapshot=s,this.unregisterOnDestroy=o?.onDestroy(()=>this.destroy())}afterRun(){this.erroredOrDestroyed=!1,this.pipelinedValue=void 0,this.snapshot?.dispose(),this.snapshot=null}destroy(){this.impl.unregister(this),this.unregisterOnDestroy?.();let t=this.view?.[Er];t&&(this.view[Er]=t.filter(e=>e!==this))}};function Qe(n,t){let e=t?.injector??u($);return ji("NgAfterNextRender"),uM(n,e,t,!0)}function dM(n){return n instanceof Function?[void 0,void 0,n,void 0]:[n.earlyRead,n.write,n.mixedReadWrite,n.read]}function uM(n,t,e,i){let r=t.get(bd);r.impl??=t.get(fp);let o=t.get(yn,null,{optional:!0}),s=e?.manualCleanup!==!0?t.get(gn):null,a=t.get(Io,null,{optional:!0}),l=new ea(r.impl,dM(n),a?.view,i,s,o?.snapshot(null));return r.impl.register(l),l}var w0=new v("",{factory:()=>({queue:new Set,isScheduled:!1,scheduler:null,injector:u(Ze)})});function C0(n,t,e){let i=n.get(w0);if(Array.isArray(t))for(let r of t)i.queue.add(r),e?.detachedLeaveAnimationFns?.push(r);else i.queue.add(t),e?.detachedLeaveAnimationFns?.push(t);i.scheduler&&i.scheduler(n)}function mM(n,t){let e=n.get(w0);if(t.detachedLeaveAnimationFns){for(let i of t.detachedLeaveAnimationFns)e.queue.delete(i);t.detachedLeaveAnimationFns=void 0}}function hM(n,t){for(let[e,i]of t)C0(n,i.animateFns)}function fy(n,t,e,i){let r=n?.[Ai]?.enter;t!==null&&r&&r.has(e.index)&&hM(i,r)}function Mo(n,t,e,i,r,o,s,a){if(r!=null){let l,c=!1;pn(r)?l=r:Jn(r)&&(c=!0,r=r[fn]);let d=Jt(r);n===0&&i!==null?(fy(a,i,o,e),s==null?h0(t,i,d):nd(t,i,d,s||null,!0)):n===1&&i!==null?(fy(a,i,o,e),nd(t,i,d,s||null,!0),lM(o,d)):n===2?(a?.[Ai]?.leave?.has(o.index)&&cM(o,d),Ys.delete(d),py(a,o,e,m=>{if(Ys.has(d)){Ys.delete(d);return}f0(t,d,c,m)})):n===3&&(Ys.delete(d),py(a,o,e,()=>{t.destroyNode(d)})),l!=null&&xM(t,n,e,l,o,i,s)}}function fM(n,t){D0(n,t),t[fn]=null,t[xt]=null}function pM(n,t,e,i,r,o){i[fn]=r,i[xt]=t,wd(n,i,e,1,r,o)}function D0(n,t){t[Nn].changeDetectionScheduler?.notify(9),wd(n,t,t[Se],2,null,null)}function gM(n){let t=n[Do];if(!t)return sf(n[Y],n);for(;t;){let e=null;if(Jn(t))e=t[Do];else{let i=t[We];i&&(e=i)}if(!e){for(;t&&!t[Xt]&&t!==n;)Jn(t)&&sf(t[Y],t),t=t[rt];t===null&&(t=n),Jn(t)&&sf(t[Y],t),e=t&&t[Xt]}t=e}}function pp(n,t){let e=n[kr],i=e.indexOf(t);e.splice(i,1)}function yd(n,t){if(Sr(t))return;let e=t[Se];e.destroyNode&&wd(n,t,e,3,null,null),gM(t)}function sf(n,t){if(Sr(t))return;let e=K(null);try{t[te]&=-129,t[te]|=256,t[Ht]&&Di(t[Ht]),bM(n,t),vM(n,t),t[Y].type===1&&t[Se].destroy();let i=t[Ti];if(i!==null&&pn(t[rt])){i!==t[rt]&&pp(i,t);let r=t[Fn];r!==null&&r.detachView(n)}_f(t)}finally{K(e)}}function py(n,t,e,i){let r=n?.[Ai];if(r==null||r.leave==null||!r.leave.has(t.index))return i(!1);n&&Rr.add(n[Xn]),C0(e,()=>{if(r.leave&&r.leave.has(t.index)){let s=r.leave.get(t.index),a=[];if(s){for(let l=0;l<s.animateFns.length;l++){let c=s.animateFns[l],{promise:d}=c();a.push(d)}r.detachedLeaveAnimationFns=void 0}r.running=Promise.allSettled(a),_M(n,i)}else n&&Rr.delete(n[Xn]),i(!1)},r)}function _M(n,t){let e=n[Ai]?.running;if(e){e.then(()=>{n[Ai].running=void 0,Rr.delete(n[Xn]),t(!0)});return}t(!1)}function vM(n,t){let e=n.cleanup,i=t[Co];if(e!==null)for(let s=0;s<e.length-1;s+=2)if(typeof e[s]=="string"){let a=e[s+3];a>=0?i[a]():i[-a].unsubscribe(),s+=2}else{let a=i[e[s+1]];e[s].call(a)}i!==null&&(t[Co]=null);let r=t[Yn];if(r!==null){t[Yn]=null;for(let s=0;s<r.length;s++){let a=r[s];a()}}let o=t[ki];if(o!==null){t[ki]=null;for(let s of o)s.destroy()}}function bM(n,t){let e;if(n!=null&&(e=n.destroyHooks)!=null)for(let i=0;i<e.length;i+=2){let r=t[e[i]];if(!(r instanceof Ar)){let o=e[i+1];if(Array.isArray(o))for(let s=0;s<o.length;s+=2){let a=r[o[s]],l=o[s+1];Me(De.LifecycleHookStart,a,l);try{l.call(a)}finally{Me(De.LifecycleHookEnd,a,l)}}else{Me(De.LifecycleHookStart,r,o);try{o.call(r)}finally{Me(De.LifecycleHookEnd,r,o)}}}}}function x0(n,t,e){return yM(n,t.parent,e)}function yM(n,t,e){let i=t;for(;i!==null&&i.type&168;)t=i,i=t.parent;if(i===null)return e[fn];if(Pn(i)){let{encapsulation:r}=n.data[i.directiveStart+i.componentOffset];if(r===bn.None||r===bn.Emulated)return null}return en(i,e)}function E0(n,t,e){return CM(n,t,e)}function wM(n,t,e){return n.type&40?en(n,e):null}var CM=wM,gy;function gp(n,t,e,i){let r=x0(n,i,t),o=t[Se],s=i.parent||t[xt],a=E0(s,i,t);if(r!=null)if(Array.isArray(e))for(let l=0;l<e.length;l++)uy(o,r,e[l],a,!1);else uy(o,r,e,a,!1);gy!==void 0&&gy(o,i,t,e,r)}function Zs(n,t){if(t!==null){let e=t.type;if(e&3)return en(t,n);if(e&4)return Mf(-1,n[t.index]);if(e&8){let i=t.child;if(i!==null)return Zs(n,i);{let r=n[t.index];return pn(r)?Mf(-1,r):Jt(r)}}else{if(e&128)return Zs(n,t.next);if(e&32)return mp(t,n)()||Jt(n[t.index]);{let i=k0(n,t);if(i!==null){if(Array.isArray(i))return i[0];let r=Si(n[Et]);return Zs(r,i)}else return Zs(n,t.next)}}}return null}function k0(n,t){if(t!==null){let i=n[Et][xt],r=t.projection;return i.projection[r]}return null}function Mf(n,t){let e=We+n+1;if(e<t.length){let i=t[e],r=i[Y].firstChild;if(r!==null)return Zs(i,r)}return t[Ri]}function _p(n,t,e,i,r,o,s){for(;e!=null;){let a=i[Zn];if(e.type===128){e=e.next;continue}let l=i[e.index],c=e.type;if(s&&t===0&&(l&&Ro(Jt(l),i),e.flags|=2),!fd(e))if(c&8)_p(n,t,e.child,i,r,o,!1),Mo(t,n,a,r,l,e,o,i);else if(c&32){let d=mp(e,i),m;for(;m=d();)Mo(t,n,a,r,m,e,o,i);Mo(t,n,a,r,l,e,o,i)}else c&16?S0(n,t,i,e,r,o):Mo(t,n,a,r,l,e,o,i);e=s?e.projectionNext:e.next}}function wd(n,t,e,i,r,o){_p(e,i,n.firstChild,t,r,o,!1)}function DM(n,t,e){let i=t[Se],r=x0(n,e,t),o=e.parent||t[xt],s=E0(o,e,t);S0(i,0,t,e,r,s)}function S0(n,t,e,i,r,o){let s=e[Et],l=s[xt].projection[i.projection];if(Array.isArray(l))for(let c=0;c<l.length;c++){let d=l[c];Mo(t,n,e[Zn],r,d,i,o,e)}else{let c=l,d=s[rt];Ky(i)&&(c.flags|=128),_p(n,t,c,d,r,o,!0)}}function xM(n,t,e,i,r,o,s){let a=i[Ri],l=Jt(i);a!==l&&Mo(t,n,e,o,a,r,s);for(let c=We;c<i.length;c++){let d=i[c];wd(d[Y],d,n,t,o,a)}}function EM(n,t,e,i,r){if(t)r?n.addClass(e,i):n.removeClass(e,i);else{let o=i.indexOf("-")===-1?void 0:Vn.DashCase;r==null?n.removeStyle(e,i,o):(typeof r=="string"&&r.endsWith("!important")&&(r=r.slice(0,-10),o|=Vn.Important),n.setStyle(e,i,r,o))}}function I0(n,t,e,i,r){let o=ti(),s=i&2;try{Oi(-1),s&&t.length>Ge&&y0(n,t,Ge,!1);let a=s?De.TemplateUpdateStart:De.TemplateCreateStart;Me(a,r,e),e(i,r)}finally{Oi(o);let a=s?De.TemplateUpdateEnd:De.TemplateCreateEnd;Me(a,r,e)}}function Cd(n,t,e){TM(n,t,e),(e.flags&64)===64&&AM(n,t,e)}function la(n,t,e=en){let i=t.localNames;if(i!==null){let r=t.index+1;for(let o=0;o<i.length;o+=2){let s=i[o+1],a=s===-1?e(t,n):n[s];n[r++]=a}}}function kM(n,t,e,i){let o=i.get(i0,n0)||e===bn.ShadowDom||e===bn.ExperimentalIsolatedShadowDom,s=n.selectRootElement(t,o);return SM(s),s}function SM(n){IM(n)}var IM=()=>null;function MM(n){return n==="class"?"className":n==="for"?"htmlFor":n==="formaction"?"formAction":n==="innerHtml"?"innerHTML":n==="readonly"?"readOnly":n==="tabindex"?"tabIndex":n}function M0(n,t,e,i,r,o){let s=t[Y];if(Dd(n,s,t,e,i)){Pn(n)&&A0(t,n.index);return}n.type&3&&(e=MM(e)),T0(n,t,e,i,r,o)}function T0(n,t,e,i,r,o){if(n.type&3){let s=en(n,t);i=o!=null?o(i,n.value||"",e):i,r.setProperty(s,e,i)}else n.type&12}function A0(n,t){let e=tn(t,n);e[te]&16||(e[te]|=64)}function TM(n,t,e){let i=e.directiveStart,r=e.directiveEnd;Pn(e)&&sM(t,e,n.data[i+e.componentOffset]),n.firstCreatePass||td(e,t);let o=e.initialInputs;for(let s=i;s<r;s++){let a=n.data[s],l=Js(t,n,s,e);if(Ro(l,t),o!==null&&NM(t,s-i,l,a,e,o),Ln(a)){let c=tn(e.index,t);c[Xe]=Js(t,n,s,e)}}}function AM(n,t,e){let i=e.directiveStart,r=e.directiveEnd,o=e.index,s=Bb();try{Oi(o);for(let a=i;a<r;a++){let l=n.data[a],c=t[a];Lc(a),(l.hostBindings!==null||l.hostVars!==0||l.hostAttrs!==null)&&RM(l,c)}}finally{Oi(-1),Lc(s)}}function RM(n,t){n.hostBindings!==null&&n.hostBindings(1,t)}function vp(n,t){let e=n.directiveRegistry,i=null;if(e)for(let r=0;r<e.length;r++){let o=e[r];_0(t,o.selectors,!1)&&(i??=[],Ln(o)?i.unshift(o):i.push(o))}return i}function OM(n,t,e,i,r,o){let s=en(n,t);R0(t[Se],s,o,n.value,e,i,r)}function R0(n,t,e,i,r,o,s){if(o==null)n.removeAttribute(t,r,e);else{let a=s==null?bh(o):s(o,i||"",r);n.setAttribute(t,r,a,e)}}function NM(n,t,e,i,r,o){let s=o[t];if(s!==null)for(let a=0;a<s.length;a+=2){let l=s[a],c=s[a+1];Sf(i,e,l,c)}}function bp(n,t,e,i,r){let o=Ge+e,s=t[Y],a=r(s,t,n,i,e);t[o]=a,ko(n,!0);let l=n.type===2;return l?(p0(t[Se],a,n),(Tb()===0||xo(n))&&Ro(a,t),Ab()):Ro(a,t),zc()&&(!l||!fd(n))&&gp(s,t,a,n),n}function yp(n){let t=n;return Gh()?Wh():(t=t.parent,ko(t,!1)),t}function FM(n,t){let e=n[Zn];if(!e)return;let i;try{i=e.get(Ni,null)}catch(r){i=null}i?.(t)}function Dd(n,t,e,i,r){let o=n.inputs?.[i],s=n.hostDirectiveInputs?.[i],a=!1;if(s)for(let l=0;l<s.length;l+=2){let c=s[l],d=s[l+1],m=t.data[c];Sf(m,e[c],d,r),a=!0}if(o)for(let l of o){let c=e[l],d=t.data[l];Sf(d,c,i,r),a=!0}return a}function PM(n,t){let e=tn(t,n),i=e[Y];LM(i,e);let r=e[fn];r!==null&&e[Cr]===null&&(e[Cr]=r0(r,e[Zn])),Me(De.ComponentStart);try{wp(i,e,e[Xe])}finally{Me(De.ComponentEnd,e[Xe])}}function LM(n,t){for(let e=t.length;e<n.blueprint.length;e++)t.push(n.blueprint[e])}function wp(n,t,e){Vc(t);try{let i=n.viewQuery;i!==null&&vf(1,i,e);let r=n.template;r!==null&&I0(n,t,r,1,e),n.firstCreatePass&&(n.firstCreatePass=!1),t[Fn]?.finishViewCreation(n),n.staticContentQueries&&o0(n,t),n.staticViewQueries&&vf(2,n.viewQuery,e);let o=n.components;o!==null&&BM(t,o)}catch(i){throw n.firstCreatePass&&(n.incompleteFirstPass=!0,n.firstCreatePass=!1),i}finally{t[te]&=-5,jc()}}function BM(n,t){for(let e=0;e<t.length;e++)PM(n,t[e])}function ca(n,t,e,i){let r=K(null);try{let o=t.tView,a=n[te]&4096?4096:16,l=dp(n,o,e,a,null,t,null,null,i?.injector??null,i?.embeddedViewInjector??null,i?.dehydratedView??null),c=n[t.index];l[Ti]=c;let d=n[Fn];return d!==null&&(l[Fn]=d.createEmbeddedView(o)),wp(o,l,e),l}finally{K(r)}}function Oo(n,t){return!t||t.firstChild===null||Ky(n)}function ta(n,t,e,i,r=!1){for(;e!==null;){if(e.type===128){e=r?e.projectionNext:e.next;continue}let o=t[e.index];o!==null&&i.push(Jt(o)),pn(o)&&O0(o,i);let s=e.type;if(s&8)ta(n,t,e.child,i);else if(s&32){let a=mp(e,t),l;for(;l=a();)i.push(l)}else if(s&16){let a=k0(t,e);if(Array.isArray(a))i.push(...a);else{let l=Si(t[Et]);ta(l[Y],l,a,i,!0)}}e=r?e.projectionNext:e.next}return i}function O0(n,t){for(let e=We;e<n.length;e++){let i=n[e],r=i[Y].firstChild;r!==null&&ta(i[Y],i,r,t)}n[Ri]!==n[fn]&&t.push(n[Ri])}function N0(n){if(n[Er]!==null){for(let t of n[Er])t.impl.addSequence(t);n[Er].length=0}}var F0=[];function VM(n){return n[Ht]??jM(n)}function jM(n){let t=F0.pop()??Object.create(HM);return t.lView=n,t}function zM(n){n.lView[Ht]!==n&&(n.lView=null,F0.push(n))}var HM=be(M({},sr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{Ir(n.lView)},consumerOnSignalRead(){this.lView[Ht]=this}});function UM(n){let t=n[Ht]??Object.create($M);return t.lView=n,t}var $M=be(M({},sr),{consumerIsAlwaysLive:!0,kind:"template",consumerMarkedDirty:n=>{let t=Si(n.lView);for(;t&&!P0(t[Y]);)t=Si(t);t&&Ph(t)},consumerOnSignalRead(){this.lView[Ht]=this}});function P0(n){return n.type!==2}function L0(n){if(n[ki]===null)return;let t=!0;for(;t;){let e=!1;for(let i of n[ki])i.dirty&&(e=!0,i.zone===null||Zone.current===i.zone?i.run():i.zone.run(()=>i.run()));t=e&&!!(n[te]&8192)}}var GM=100;function B0(n,t=0){let i=n[Nn].rendererFactory,r=!1;r||i.begin?.();try{WM(n,t)}finally{r||i.end?.()}}function WM(n,t){let e=qh();try{Fs(!0),Tf(n,t);let i=0;for(;Gs(n);){if(i===GM)throw new D(103,!1);i++,Tf(n,1)}}finally{Fs(e)}}function qM(n,t,e,i){if(Sr(t))return;let r=t[te],o=!1,s=!1;Vc(t);let a=!0,l=null,c=null;o||(P0(n)?(c=VM(t),l=Ci(c)):Ml()===null?(a=!1,c=UM(t),l=Ci(c)):t[Ht]&&(Di(t[Ht]),t[Ht]=null));try{Fh(t),Fb(n.bindingStartIndex),e!==null&&I0(n,t,e,2,i);let d=(r&3)===3;if(!o)if(d){let h=n.preOrderCheckHooks;h!==null&&qc(t,h,null)}else{let h=n.preOrderHooks;h!==null&&Qc(t,h,0,null),rf(t,0)}if(s||QM(t),L0(t),V0(t,0),n.contentQueries!==null&&o0(n,t),!o)if(d){let h=n.contentCheckHooks;h!==null&&qc(t,h)}else{let h=n.contentHooks;h!==null&&Qc(t,h,1),rf(t,1)}YM(n,t);let m=n.components;m!==null&&z0(t,m,0);let f=n.viewQuery;if(f!==null&&vf(2,f,i),!o)if(d){let h=n.viewCheckHooks;h!==null&&qc(t,h)}else{let h=n.viewHooks;h!==null&&Qc(t,h,2),rf(t,2)}if(n.firstUpdatePass===!0&&(n.firstUpdatePass=!1),t[Rc]){for(let h of t[Rc])h();t[Rc]=null}o||(N0(t),t[te]&=-73)}catch(d){throw o||Ir(t),d}finally{c!==null&&(ar(c,l),a&&zM(c)),jc()}}function V0(n,t){for(let e=Zy(n);e!==null;e=Xy(e))for(let i=We;i<e.length;i++){let r=e[i];j0(r,t)}}function QM(n){for(let t=Zy(n);t!==null;t=Xy(t)){if(!(t[te]&2))continue;let e=t[kr];for(let i=0;i<e.length;i++){let r=e[i];Ph(r)}}}function KM(n,t,e){Me(De.ComponentStart);let i=tn(t,n);try{j0(i,e)}finally{Me(De.ComponentEnd,i[Xe])}}function j0(n,t){Nc(n)&&Tf(n,t)}function Tf(n,t){let i=n[Y],r=n[te],o=n[Ht],s=!!(t===0&&r&16);if(s||=!!(r&64&&t===0),s||=!!(r&1024),s||=!!(o?.dirty&&io(o)),s||=!1,o&&(o.dirty=!1),n[te]&=-9217,s)qM(i,n,i.template,n[Xe]);else if(r&8192){let a=K(null);try{L0(n),V0(n,1);let l=i.components;l!==null&&z0(n,l,1),N0(n)}finally{K(a)}}}function z0(n,t,e){for(let i=0;i<t.length;i++)KM(n,t[i],e)}function YM(n,t){let e=n.hostBindingOpCodes;if(e!==null)try{for(let i=0;i<e.length;i++){let r=e[i];if(r<0)Oi(~r);else{let o=r,s=e[++i],a=e[++i];Lb(s,o);let l=t[o];Me(De.HostBindingsUpdateStart,l);try{a(2,l)}finally{Me(De.HostBindingsUpdateEnd,l)}}}}finally{Oi(-1)}}function Cp(n,t){let e=qh()?64:1088;for(n[Nn].changeDetectionScheduler?.notify(t);n;){n[te]|=e;let i=Si(n);if(Eo(n)&&!i)return n;n=i}return null}function H0(n,t,e,i){return[n,!0,0,t,null,i,null,e,null,null]}function U0(n,t){let e=We+t;if(e<n.length)return n[e]}function da(n,t,e,i=!0){let r=t[Y];if(ZM(r,t,n,e),i){let s=Mf(e,n),a=t[Se],l=a.parentNode(n[Ri]);l!==null&&pM(r,n[xt],a,t,l,s)}let o=t[Cr];o!==null&&o.firstChild!==null&&(o.firstChild=null)}function $0(n,t){let e=na(n,t);return e!==void 0&&yd(e[Y],e),e}function na(n,t){if(n.length<=We)return;let e=We+t,i=n[e];if(i){let r=i[Ti];r!==null&&r!==n&&pp(r,i),t>0&&(n[e-1][Xt]=i[Xt]);let o=zs(n,We+t);fM(i[Y],i);let s=o[Fn];s!==null&&s.detachView(o[Y]),i[rt]=null,i[Xt]=null,i[te]&=-129}return i}function ZM(n,t,e,i){let r=We+i,o=e.length;i>0&&(e[r-1][Xt]=t),i<o-We?(t[Xt]=e[r],Ch(e,We+i,t)):(e.push(t),t[Xt]=null),t[rt]=e;let s=t[Ti];s!==null&&e!==s&&G0(s,t);let a=t[Fn];a!==null&&a.insertView(n),Fc(t),t[te]|=128}function G0(n,t){let e=n[kr],i=t[rt];if(Jn(i))n[te]|=2;else{let r=i[rt][Et];t[Et]!==r&&(n[te]|=2)}e===null?n[kr]=[t]:e.push(t)}var Li=class{_lView;_cdRefInjectingView;_appRef=null;_attachedToViewContainer=!1;exhaustive;get rootNodes(){let t=this._lView,e=t[Y];return ta(e,t,e.firstChild,[])}constructor(t,e){this._lView=t,this._cdRefInjectingView=e}get context(){return this._lView[Xe]}set context(t){this._lView[Xe]=t}get destroyed(){return Sr(this._lView)}destroy(){if(this._appRef)this._appRef.detachView(this);else if(this._attachedToViewContainer){let t=this._lView[rt];if(pn(t)){let e=t[$s],i=e?e.indexOf(this):-1;i>-1&&(na(t,i),zs(e,i))}this._attachedToViewContainer=!1}yd(this._lView[Y],this._lView)}onDestroy(t){Lh(this._lView,t)}markForCheck(){Cp(this._cdRefInjectingView||this._lView,4)}detach(){this._lView[te]&=-129}reattach(){Fc(this._lView),this._lView[te]|=128}detectChanges(){this._lView[te]|=1024,B0(this._lView)}checkNoChanges(){}attachToViewContainerRef(){if(this._appRef)throw new D(902,!1);this._attachedToViewContainer=!0}detachFromAppRef(){this._appRef=null;let t=Eo(this._lView),e=this._lView[Ti];e!==null&&!t&&pp(e,this._lView),D0(this._lView[Y],this._lView)}attachToAppRef(t){if(this._attachedToViewContainer)throw new D(902,!1);this._appRef=t;let e=Eo(this._lView),i=this._lView[Ti];i!==null&&!e&&G0(i,this._lView),Fc(this._lView)}};var Pe=(()=>{class n{_declarationLView;_declarationTContainer;elementRef;static __NG_ELEMENT_ID__=XM;constructor(e,i,r){this._declarationLView=e,this._declarationTContainer=i,this.elementRef=r}get ssrId(){return this._declarationTContainer.tView?.ssrId||null}createEmbeddedView(e,i){return this.createEmbeddedViewImpl(e,i)}createEmbeddedViewImpl(e,i,r){let o=ca(this._declarationLView,this._declarationTContainer,e,{embeddedViewInjector:i,dehydratedView:r});return new Li(o)}}return n})();function XM(){return xd(ot(),ne())}function xd(n,t){return n.type&4?new Pe(t,n,Po(n,t)):null}function Lo(n,t,e,i,r){let o=n.data[t];if(o===null)o=JM(n,t,e,i,r),Pb()&&(o.flags|=32);else if(o.type&64){o.type=e,o.value=i,o.attrs=r;let s=Rb();o.injectorIndex=s===null?-1:s.injectorIndex}return ko(o,!0),o}function JM(n,t,e,i,r){let o=$h(),s=Gh(),a=s?o:o&&o.parent,l=n.data[t]=tT(n,a,e,t,i,r);return eT(n,l,o,s),l}function eT(n,t,e,i){n.firstChild===null&&(n.firstChild=t),e!==null&&(i?e.child==null&&t.parent!==null&&(e.child=t):e.next===null&&(e.next=t,t.prev=e))}function tT(n,t,e,i,r,o){let s=t?t.injectorIndex:-1,a=0;return zh()&&(a|=128),{type:e,index:i,insertBeforeIndex:null,injectorIndex:s,directiveStart:-1,directiveEnd:-1,directiveStylingLast:-1,componentOffset:-1,controlDirectiveIndex:-1,customControlIndex:-1,propertyBindings:null,flags:a,providerIndexes:0,value:r,namespace:Zh(),attrs:o,mergedAttrs:null,localNames:null,initialInputs:null,inputs:null,hostDirectiveInputs:null,outputs:null,hostDirectiveOutputs:null,directiveToIndex:null,tView:null,next:null,prev:null,projectionNext:null,child:null,parent:t,projection:null,styles:null,stylesWithoutHost:null,residualStyles:void 0,classes:null,classesWithoutHost:null,residualClasses:void 0,classBindings:0,styleBindings:0}}function nT(n){let t=n[Ah]??[],i=n[rt][Se],r=[];for(let o of t)o.data[t0]!==void 0?r.push(o):iT(o,i);n[Ah]=r}function iT(n,t){let e=0,i=n.firstChild;if(i){let r=n.data[e0];for(;e<r;){let o=i.nextSibling;f0(t,i,!1),i=o,e++}}}var rT=()=>null,oT=()=>null;function id(n,t){return rT(n,t)}function W0(n,t,e){return oT(n,t,e)}var q0=class{},Ed=class{},Af=class{resolveComponentFactory(t){throw new D(917,!1)}},ua=class{static NULL=new Af},qe=class{},Ke=(()=>{class n{destroyNode=null;static __NG_ELEMENT_ID__=()=>sT()}return n})();function sT(){let n=ne(),t=ot(),e=tn(t.index,n);return(Jn(e)?e:n)[Se]}var Q0=(()=>{class n{static \u0275prov=w({token:n,providedIn:"root",factory:()=>null})}return n})();var Yc={},Rf=class{injector;parentInjector;constructor(t,e){this.injector=t,this.parentInjector=e}get(t,e,i){let r=this.injector.get(t,Yc,i);return r!==Yc||e===Yc?r:this.parentInjector.get(t,e,i)}};function rd(n,t,e){let i=e?n.styles:null,r=e?n.classes:null,o=0;if(t!==null)for(let s=0;s<t.length;s++){let a=t[s];if(typeof a=="number")o=a;else if(o==1)r=xc(r,a);else if(o==2){let l=a,c=t[++s];i=xc(i,l+": "+c+";")}}e?n.styles=i:n.stylesWithoutHost=i,e?n.classes=r:n.classesWithoutHost=r}function q(n,t=0){let e=ne();if(e===null)return B(n,t);let i=ot();return Gy(i,e,mt(n),t)}function K0(n,t,e,i,r){let o=i===null?null:{"":-1},s=r(n,e);if(s!==null){let a=s,l=null,c=null;for(let d of s)if(d.resolveHostDirectives!==null){[a,l,c]=d.resolveHostDirectives(s);break}cT(n,t,e,a,o,l,c)}o!==null&&i!==null&&aT(e,i,o)}function aT(n,t,e){let i=n.localNames=[];for(let r=0;r<t.length;r+=2){let o=e[t[r+1]];if(o==null)throw new D(-301,!1);i.push(t[r],o)}}function lT(n,t,e){t.componentOffset=e,(n.components??=[]).push(t.index)}function cT(n,t,e,i,r,o,s){let a=i.length,l=null;for(let f=0;f<a;f++){let h=i[f];l===null&&Ln(h)&&(l=h,lT(n,e,f)),pf(td(e,t),n,h.type)}pT(e,n.data.length,a),l?.viewProvidersResolver&&l.viewProvidersResolver(l);for(let f=0;f<a;f++){let h=i[f];h.providersResolver&&h.providersResolver(h)}let c=!1,d=!1,m=b0(n,t,a,null);a>0&&(e.directiveToIndex=new Map);for(let f=0;f<a;f++){let h=i[f];if(e.mergedAttrs=Ao(e.mergedAttrs,h.hostAttrs),uT(n,e,t,m,h),fT(m,h,r),s!==null&&s.has(h)){let[b,C]=s.get(h);e.directiveToIndex.set(h.type,[m,b+e.directiveStart,C+e.directiveStart])}else(o===null||!o.has(h))&&e.directiveToIndex.set(h.type,m);h.contentQueries!==null&&(e.flags|=4),(h.hostBindings!==null||h.hostAttrs!==null||h.hostVars!==0)&&(e.flags|=64);let p=h.type.prototype;!c&&(p.ngOnChanges||p.ngOnInit||p.ngDoCheck)&&((n.preOrderHooks??=[]).push(e.index),c=!0),!d&&(p.ngOnChanges||p.ngDoCheck)&&((n.preOrderCheckHooks??=[]).push(e.index),d=!0),m++}dT(n,e,o)}function dT(n,t,e){for(let i=t.directiveStart;i<t.directiveEnd;i++){let r=n.data[i];if(e===null||!e.has(r))_y(0,t,r,i),_y(1,t,r,i),by(t,i,!1);else{let o=e.get(r);vy(0,t,o,i),vy(1,t,o,i),by(t,i,!0)}}}function _y(n,t,e,i){let r=n===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s;n===0?s=t.inputs??={}:s=t.outputs??={},s[o]??=[],s[o].push(i),Y0(t,o)}}function vy(n,t,e,i){let r=n===0?e.inputs:e.outputs;for(let o in r)if(r.hasOwnProperty(o)){let s=r[o],a;n===0?a=t.hostDirectiveInputs??={}:a=t.hostDirectiveOutputs??={},a[s]??=[],a[s].push(i,o),Y0(t,s)}}function Y0(n,t){t==="class"?n.flags|=8:t==="style"&&(n.flags|=16)}function by(n,t,e){let{attrs:i,inputs:r,hostDirectiveInputs:o}=n;if(i===null||!e&&r===null||e&&o===null||lp(n)){n.initialInputs??=[],n.initialInputs.push(null);return}let s=null,a=0;for(;a<i.length;){let l=i[a];if(l===0){a+=4;continue}else if(l===5){a+=2;continue}else if(typeof l=="number")break;if(!e&&r.hasOwnProperty(l)){let c=r[l];for(let d of c)if(d===t){s??=[],s.push(l,i[a+1]);break}}else if(e&&o.hasOwnProperty(l)){let c=o[l];for(let d=0;d<c.length;d+=2)if(c[d]===t){s??=[],s.push(c[d+1],i[a+1]);break}}a+=2}n.initialInputs??=[],n.initialInputs.push(s)}function uT(n,t,e,i,r){n.data[i]=r;let o=r.factory||(r.factory=vr(r.type,!0)),s=new Ar(o,Ln(r),q,null);n.blueprint[i]=s,e[i]=s,mT(n,t,i,b0(n,e,r.hostVars,rn),r)}function mT(n,t,e,i,r){let o=r.hostBindings;if(o){let s=n.hostBindingOpCodes;s===null&&(s=n.hostBindingOpCodes=[]);let a=~t.index;hT(s)!=a&&s.push(a),s.push(e,i,o)}}function hT(n){let t=n.length;for(;t>0;){let e=n[--t];if(typeof e=="number"&&e<0)return e}return 0}function fT(n,t,e){if(e){if(t.exportAs)for(let i=0;i<t.exportAs.length;i++)e[t.exportAs[i]]=n;Ln(t)&&(e[""]=n)}}function pT(n,t,e){n.flags|=1,n.directiveStart=t,n.directiveEnd=t+e,n.providerIndexes=t}function Dp(n,t,e,i,r,o,s,a){let l=t[Y],c=l.consts,d=nn(c,s),m=Lo(l,n,e,i,d);return o&&K0(l,t,m,nn(c,a),r),m.mergedAttrs=Ao(m.mergedAttrs,m.attrs),m.attrs!==null&&rd(m,m.attrs,!1),m.mergedAttrs!==null&&rd(m,m.mergedAttrs,!0),l.queries!==null&&l.queries.elementStart(l,m),m}function xp(n,t){Py(n,t),Rh(t)&&n.queries.elementEnd(t)}function gT(n,t,e,i,r,o){let s=t.consts,a=nn(s,r),l=Lo(t,n,e,i,a);if(l.mergedAttrs=Ao(l.mergedAttrs,l.attrs),o!=null){let c=nn(s,o);l.localNames=[];for(let d=0;d<c.length;d+=2)l.localNames.push(c[d],-1)}return l.attrs!==null&&rd(l,l.attrs,!1),l.mergedAttrs!==null&&rd(l,l.mergedAttrs,!0),t.queries!==null&&t.queries.elementStart(t,l),l}function Ep(n){return X0(n)?Array.isArray(n)||!(n instanceof Map)&&Symbol.iterator in n:!1}function Z0(n,t){if(Array.isArray(n))for(let e=0;e<n.length;e++)t(n[e]);else{let e=n[Symbol.iterator](),i;for(;!(i=e.next()).done;)t(i.value)}}function X0(n){return n!==null&&(typeof n=="function"||typeof n=="object")}function _T(n,t,e){return n[t]=e}function vT(n,t){return n[t]}function zn(n,t,e){if(e===rn)return!1;let i=n[t];return Object.is(i,e)?!1:(n[t]=e,!0)}function Zc(n,t,e){return function i(r){let o=i.__ngNativeEl__;o!==void 0&&pI(r,o);let s=Pn(n)?tn(n.index,t):t;Cp(s,5);let a=t[Xe],l=yy(t,a,e,r),c=i.__ngNextListenerFn__;for(;c;)l=yy(t,a,c,r)&&l,c=c.__ngNextListenerFn__;return l}}function yy(n,t,e,i){let r=K(null);try{return Me(De.OutputStart,t,e),e(i)!==!1}catch(o){return FM(n,o),!1}finally{Me(De.OutputEnd,t,e),K(r)}}function J0(n,t,e,i,r,o,s,a){let l=xo(n),c=!1,d=null;if(!i&&l&&(d=yT(t,e,o,n.index)),d!==null){let m=d.__ngLastListenerFn__||d;m.__ngNextListenerFn__=s,d.__ngLastListenerFn__=s,c=!0}else{let m=en(n,e),f=i?i(m):m;_I(e,f,o,a),i||(a.__ngNativeEl__=m);let h=r.listen(f,o,a);if(!bT(o)){let p=i?b=>i(Jt(b[n.index])):n.index;ew(p,t,e,o,a,h,!1)}}return c}function bT(n){return n.startsWith("animation")||n.startsWith("transition")}function yT(n,t,e,i){let r=n.cleanup;if(r!=null)for(let o=0;o<r.length-1;o+=2){let s=r[o];if(s===e&&r[o+1]===i){let a=t[Co],l=r[o+2];return a&&a.length>l?a[l]:null}typeof s=="string"&&(o+=2)}return null}function ew(n,t,e,i,r,o,s){let a=t.firstCreatePass?Vh(t):null,l=Bh(e),c=l.length;l.push(r,o),a&&a.push(i,n,c,(c+1)*(s?-1:1))}function wy(n,t,e,i,r,o){let s=t[e],a=t[Y],c=a.data[e].outputs[i],m=s[c].subscribe(o);ew(n.index,a,t,r,o,m,!0)}var Of=Symbol("BINDING");function tw(n){return n.debugInfo?.className||n.type.name||null}var od=class extends ua{ngModule;constructor(t){super(),this.ngModule=t}resolveComponentFactory(t){let e=Ii(t);return new No(e,this.ngModule)}};function wT(n){return Object.keys(n).map(t=>{let[e,i,r]=n[t],o={propName:e,templateName:t,isSignal:(i&_d.SignalBased)!==0};return r&&(o.transform=r),o})}function CT(n){return Object.keys(n).map(t=>({propName:n[t],templateName:t}))}function DT(n,t,e){let i=t instanceof Ze?t:t?.injector;return i&&n.getStandaloneInjector!==null&&(i=n.getStandaloneInjector(i)||i),i?new Rf(e,i):e}function xT(n){let t=n.get(qe,null);if(t===null)throw new D(407,!1);let e=n.get(Q0,null),i=n.get(On,null),r=n.get(yn,null,{optional:!0});return{rendererFactory:t,sanitizer:e,changeDetectionScheduler:i,ngReflect:!1,tracingService:r}}function ET(n,t){let e=nw(n);return m0(t,e,e==="svg"?Oh:e==="math"?Db:null)}function kT(n){if(n?.toLowerCase()==="script")throw new D(905,!1)}function nw(n){return(n.selectors[0][0]||"div").toLowerCase()}var No=class extends Ed{componentDef;ngModule;selector;componentType;ngContentSelectors;isBoundToModule;cachedInputs=null;cachedOutputs=null;get inputs(){return this.cachedInputs??=wT(this.componentDef.inputs),this.cachedInputs}get outputs(){return this.cachedOutputs??=CT(this.componentDef.outputs),this.cachedOutputs}constructor(t,e){super(),this.componentDef=t,this.ngModule=e,this.componentType=t.type,this.selector=nM(t.selectors),this.ngContentSelectors=t.ngContentSelectors??[],this.isBoundToModule=!!e}create(t,e,i,r,o,s){Me(De.DynamicComponentStart);let a=K(null);try{let l=this.componentDef,c=DT(l,r||this.ngModule,t),d=xT(c),m=d.tracingService;return m&&m.componentCreate?m.componentCreate(tw(l),()=>this.createComponentRef(d,c,e,i,o,s)):this.createComponentRef(d,c,e,i,o,s)}finally{K(a)}}createComponentRef(t,e,i,r,o,s){let a=this.componentDef,l=ST(r,a,s,o),c=t.rendererFactory.createRenderer(null,a),d=r?kM(c,r,a.encapsulation,e):ET(a,c);kT(d?.tagName);let m=s?.some(Cy)||o?.some(p=>typeof p!="function"&&p.bindings.some(Cy)),f=dp(null,l,null,512|v0(a),null,null,t,c,e,null,r0(d,e,!0));f[Ge]=d,Vc(f);let h=null;try{let p=Dp(Ge,f,2,"#host",()=>l.directiveRegistry,!0,0);p0(c,d,p),Ro(d,f),Cd(l,f,p),tp(l,p,f),xp(l,p),i!==void 0&&MT(p,this.ngContentSelectors,i),h=tn(p.index,f),f[Xe]=h[Xe],wp(l,f,null)}catch(p){throw h!==null&&_f(h),_f(f),p}finally{Me(De.DynamicComponentEnd),jc()}return new sd(this.componentType,f,!!m)}};function ST(n,t,e,i){let r=n?["ng-version","21.2.25"]:iM(t.selectors[0]),o=null,s=null,a=0;if(e)for(let d of e)a+=d[Of].requiredVars,d.create&&(d.targetIdx=0,(o??=[]).push(d)),d.update&&(d.targetIdx=0,(s??=[]).push(d));if(i)for(let d=0;d<i.length;d++){let m=i[d];if(typeof m!="function")for(let f of m.bindings){a+=f[Of].requiredVars;let h=d+1;f.create&&(f.targetIdx=h,(o??=[]).push(f)),f.update&&(f.targetIdx=h,(s??=[]).push(f))}}let l=[t];if(i)for(let d of i){let m=typeof d=="function"?d:d.type,f=vh(m);l.push(f)}return cp(0,null,IT(o,s),1,a,l,null,null,null,[r],null)}function IT(n,t){return!n&&!t?null:e=>{if(e&1&&n)for(let i of n)i.create();if(e&2&&t)for(let i of t)i.update()}}function Cy(n){let t=n[Of].kind;return t==="input"||t==="twoWay"}var sd=class extends q0{_rootLView;_hasInputBindings;instance;hostView;changeDetectorRef;componentType;location;previousInputValues=null;_tNode;constructor(t,e,i){super(),this._rootLView=e,this._hasInputBindings=i,this._tNode=Oc(e[Y],Ge),this.location=Po(this._tNode,e),this.instance=tn(this._tNode.index,e)[Xe],this.hostView=this.changeDetectorRef=new Li(e,void 0),this.componentType=t}setInput(t,e){this._hasInputBindings;let i=this._tNode;if(this.previousInputValues??=new Map,this.previousInputValues.has(t)&&Object.is(this.previousInputValues.get(t),e))return;let r=this._rootLView,o=Dd(i,r[Y],r,t,e);this.previousInputValues.set(t,e);let s=tn(i.index,r);Cp(s,1)}get injector(){return new Tr(this._tNode,this._rootLView)}destroy(){this.hostView.destroy()}onDestroy(t){this.hostView.onDestroy(t)}};function MT(n,t,e){let i=n.projection=[];for(let r=0;r<t.length;r++){let o=e[r];i.push(o!=null&&o.length?Array.from(o):null)}}var tt=(()=>{class n{static __NG_ELEMENT_ID__=TT}return n})();function TT(){let n=ot();return iw(n,ne())}var Nf=class n extends tt{_lContainer;_hostTNode;_hostLView;constructor(t,e,i){super(),this._lContainer=t,this._hostTNode=e,this._hostLView=i}get element(){return Po(this._hostTNode,this._hostLView)}get injector(){return new Tr(this._hostTNode,this._hostLView)}get parentInjector(){let t=Zf(this._hostTNode,this._hostLView);if(Vy(t)){let e=ed(t,this._hostLView),i=Jc(t),r=e[Y].data[i+8];return new Tr(r,e)}else return new Tr(null,this._hostLView)}clear(){for(;this.length>0;)this.remove(this.length-1)}get(t){let e=Dy(this._lContainer);return e!==null&&e[t]||null}get length(){return this._lContainer.length-We}createEmbeddedView(t,e,i){let r,o;typeof i=="number"?r=i:i!=null&&(r=i.index,o=i.injector);let s=id(this._lContainer,t.ssrId),a=t.createEmbeddedViewImpl(e||{},o,s);return this.insertImpl(a,r,Oo(this._hostTNode,s)),a}createComponent(t,e,i,r,o,s,a){let l=t&&!US(t),c;if(l)c=e;else{let C=e||{};c=C.index,i=C.injector,r=C.projectableNodes,o=C.environmentInjector||C.ngModuleRef,s=C.directives,a=C.bindings}let d=l?t:new No(Ii(t)),m=i||this.parentInjector;if(!o&&d.ngModule==null){let x=(l?m:this.parentInjector).get(Ze,null);x&&(o=x)}let f=Ii(d.componentType??{}),h=id(this._lContainer,f?.id??null),p=h?.firstChild??null,b=d.create(m,r,p,o,s,a);return this.insertImpl(b.hostView,c,Oo(this._hostTNode,h)),b}insert(t,e){return this.insertImpl(t,e,!0)}insertImpl(t,e,i){let r=t._lView;if(kb(r)){let a=this.indexOf(t);if(a!==-1)this.detach(a);else{let l=r[rt],c=new n(l,l[xt],l[rt]);c.detach(c.indexOf(t))}}let o=this._adjustIndex(e),s=this._lContainer;return da(s,r,o,i),t.attachToViewContainerRef(),Ch(af(s),o,t),t}move(t,e){return this.insert(t,e)}indexOf(t){let e=Dy(this._lContainer);return e!==null?e.indexOf(t):-1}remove(t){let e=this._adjustIndex(t,-1),i=na(this._lContainer,e);i&&(zs(af(this._lContainer),e),yd(i[Y],i))}detach(t){let e=this._adjustIndex(t,-1),i=na(this._lContainer,e);return i&&zs(af(this._lContainer),e)!=null?new Li(i):null}_adjustIndex(t,e=0){return t??this.length+e}};function Dy(n){return n[$s]}function af(n){return n[$s]||(n[$s]=[])}function iw(n,t){let e,i=t[n.index];return pn(i)?e=i:(e=H0(i,t,null,n),t[n.index]=e,up(t,e)),RT(e,t,n,i),new Nf(e,n,t)}function AT(n,t){let e=n[Se],i=e.createComment(""),r=en(t,n),o=e.parentNode(r);return nd(e,o,i,e.nextSibling(r),!1),i}var RT=FT,OT=()=>!1;function NT(n,t,e){return OT(n,t,e)}function FT(n,t,e,i){if(n[Ri])return;let r;e.type&8?r=Jt(i):r=AT(t,e),n[Ri]=r}var Ff=class n{queryList;matches=null;constructor(t){this.queryList=t}clone(){return new n(this.queryList)}setDirty(){this.queryList.setDirty()}},Pf=class n{queries;constructor(t=[]){this.queries=t}createEmbeddedView(t){let e=t.queries;if(e!==null){let i=t.contentQueries!==null?t.contentQueries[0]:e.length,r=[];for(let o=0;o<i;o++){let s=e.getByIndex(o),a=this.queries[s.indexInDeclarationView];r.push(a.clone())}return new n(r)}return null}insertView(t){this.dirtyQueriesWithMatches(t)}detachView(t){this.dirtyQueriesWithMatches(t)}finishViewCreation(t){this.dirtyQueriesWithMatches(t)}dirtyQueriesWithMatches(t){for(let e=0;e<this.queries.length;e++)Sp(t,e).matches!==null&&this.queries[e].setDirty()}},ad=class{flags;read;predicate;constructor(t,e,i=null){this.flags=e,this.read=i,typeof t=="string"?this.predicate=jT(t):this.predicate=t}},Lf=class n{queries;constructor(t=[]){this.queries=t}elementStart(t,e){for(let i=0;i<this.queries.length;i++)this.queries[i].elementStart(t,e)}elementEnd(t){for(let e=0;e<this.queries.length;e++)this.queries[e].elementEnd(t)}embeddedTView(t){let e=null;for(let i=0;i<this.length;i++){let r=e!==null?e.length:0,o=this.getByIndex(i).embeddedTView(t,r);o&&(o.indexInDeclarationView=i,e!==null?e.push(o):e=[o])}return e!==null?new n(e):null}template(t,e){for(let i=0;i<this.queries.length;i++)this.queries[i].template(t,e)}getByIndex(t){return this.queries[t]}get length(){return this.queries.length}track(t){this.queries.push(t)}},Bf=class n{metadata;matches=null;indexInDeclarationView=-1;crossesNgTemplate=!1;_declarationNodeIndex;_appliesToNextNode=!0;constructor(t,e=-1){this.metadata=t,this._declarationNodeIndex=e}elementStart(t,e){this.isApplyingToNode(e)&&this.matchTNode(t,e)}elementEnd(t){this._declarationNodeIndex===t.index&&(this._appliesToNextNode=!1)}template(t,e){this.elementStart(t,e)}embeddedTView(t,e){return this.isApplyingToNode(t)?(this.crossesNgTemplate=!0,this.addMatch(-t.index,e),new n(this.metadata)):null}isApplyingToNode(t){if(this._appliesToNextNode&&(this.metadata.flags&1)!==1){let e=this._declarationNodeIndex,i=t.parent;for(;i!==null&&i.type&8&&i.index!==e;)i=i.parent;return e===(i!==null?i.index:-1)}return this._appliesToNextNode}matchTNode(t,e){let i=this.metadata.predicate;if(Array.isArray(i))for(let r=0;r<i.length;r++){let o=i[r];this.matchTNodeWithReadOption(t,e,PT(e,o)),this.matchTNodeWithReadOption(t,e,Kc(e,t,o,!1,!1))}else i===Pe?e.type&4&&this.matchTNodeWithReadOption(t,e,-1):this.matchTNodeWithReadOption(t,e,Kc(e,t,i,!1,!1))}matchTNodeWithReadOption(t,e,i){if(i!==null){let r=this.metadata.read;if(r!==null)if(r===P||r===tt||r===Pe&&e.type&4)this.addMatch(e.index,-2);else{let o=Kc(e,t,r,!1,!1);o!==null&&this.addMatch(e.index,o)}else this.addMatch(e.index,i)}}addMatch(t,e){this.matches===null?this.matches=[t,e]:this.matches.push(t,e)}};function PT(n,t){let e=n.localNames;if(e!==null){for(let i=0;i<e.length;i+=2)if(e[i]===t)return e[i+1]}return null}function LT(n,t){return n.type&11?Po(n,t):n.type&4?xd(n,t):null}function BT(n,t,e,i){return e===-1?LT(t,n):e===-2?VT(n,t,i):Js(n,n[Y],e,t)}function VT(n,t,e){if(e===P)return Po(t,n);if(e===Pe)return xd(t,n);if(e===tt)return iw(t,n)}function rw(n,t,e,i){let r=t[Fn].queries[i];if(r.matches===null){let o=n.data,s=e.matches,a=[];for(let l=0;s!==null&&l<s.length;l+=2){let c=s[l];if(c<0)a.push(null);else{let d=o[c];a.push(BT(t,d,s[l+1],e.metadata.read))}}r.matches=a}return r.matches}function Vf(n,t,e,i){let r=n.queries.getByIndex(e),o=r.matches;if(o!==null){let s=rw(n,t,r,e);for(let a=0;a<o.length;a+=2){let l=o[a];if(l>0)i.push(s[a/2]);else{let c=o[a+1],d=t[-l];for(let m=We;m<d.length;m++){let f=d[m];f[Ti]===f[rt]&&Vf(f[Y],f,c,i)}if(d[kr]!==null){let m=d[kr];for(let f=0;f<m.length;f++){let h=m[f];Vf(h[Y],h,c,i)}}}}}return i}function kp(n,t){return n[Fn].queries[t].queryList}function ow(n,t,e){let i=new Pi((e&4)===4);return Mb(n,t,i,i.destroy),(t[Fn]??=new Pf).queries.push(new Ff(i))-1}function sw(n,t,e){let i=Ne();return i.firstCreatePass&&(lw(i,new ad(n,t,e),-1),(t&2)===2&&(i.staticViewQueries=!0)),ow(i,ne(),t)}function aw(n,t,e,i){let r=Ne();if(r.firstCreatePass){let o=ot();lw(r,new ad(t,e,i),o.index),zT(r,n),(e&2)===2&&(r.staticContentQueries=!0)}return ow(r,ne(),e)}function jT(n){return n.split(",").map(t=>t.trim())}function lw(n,t,e){n.queries===null&&(n.queries=new Lf),n.queries.track(new Bf(t,e))}function zT(n,t){let e=n.contentQueries||(n.contentQueries=[]),i=e.length?e[e.length-1]:-1;t!==i&&e.push(n.queries.length-1,t)}function Sp(n,t){return n.queries.getByIndex(t)}function cw(n,t){let e=n[Y],i=Sp(e,t);return i.crossesNgTemplate?Vf(e,n,t,[]):rw(e,n,i,t)}function dw(n,t,e){let i,r=ys(()=>{i._dirtyCounter();let o=HT(i,n);if(t&&o===void 0)throw new D(-951,!1);return o});return i=r[lt],i._dirtyCounter=Oe(0),i._flatValue=void 0,r}function Ip(n){return dw(!0,!1,n)}function Mp(n){return dw(!0,!0,n)}function uw(n,t){let e=n[lt];e._lView=ne(),e._queryIndex=t,e._queryList=kp(e._lView,t),e._queryList.onDirty(()=>e._dirtyCounter.update(i=>i+1))}function HT(n,t){let e=n._lView,i=n._queryIndex;if(e===void 0||i===void 0||e[te]&4)return t?void 0:vt;let r=kp(e,i),o=cw(e,i);return r.reset(o,Qy),t?r.first:r._changesDetected||n._flatValue===void 0?n._flatValue=r.toArray():n._flatValue}var jf=new Map,UT=new Set;function Tp(n){return wi(this,null,function*(){let t=jf;jf=new Map;let e=new Map;function i(o){let s=e.get(o);if(s)return s;let a=n(o).then(l=>$T(o,l));return e.set(o,a),a}let r=Array.from(t).map(a=>wi(null,[a],function*([o,s]){if(s.styleUrl&&s.styleUrls?.length)throw new Error("@Component cannot define both `styleUrl` and `styleUrls`. Use `styleUrl` if the component has one stylesheet, or `styleUrls` if it has multiple");let l=[];s.templateUrl&&l.push(i(s.templateUrl).then(f=>{s.template=f}));let c=typeof s.styles=="string"?[s.styles]:s.styles??[];s.styles=c;let{styleUrl:d,styleUrls:m}=s;if(d&&(m=[d],s.styleUrl=void 0),m?.length){let f=Promise.all(m.map(h=>i(h))).then(h=>{c.push(...h),s.styleUrls=void 0});l.push(f)}yield Promise.all(l),UT.delete(o)}));yield Promise.all(r)})}function mw(){return jf.size===0}function $T(n,t){return wi(this,null,function*(){if(typeof t=="string")return t;if(t.status!==void 0&&t.status!==200)throw new D(918,!1);return t.text()})}var jn=class{},hw=class{};var ia=class extends jn{ngModuleType;_parent;_bootstrapComponents=[];_r3Injector;instance;destroyCbs=[];componentFactoryResolver=new od(this);constructor(t,e,i,r=!0){super(),this.ngModuleType=t,this._parent=e;let o=cb(t);this._bootstrapComponents=GI(o.bootstrap),this._r3Injector=Xh(t,e,[{provide:jn,useValue:this},{provide:ua,useValue:this.componentFactoryResolver},...i],Bs(t),new Set(["environment"])),r&&this.resolveInjectorInitializers()}resolveInjectorInitializers(){this._r3Injector.resolveInjectorInitializers(),this.instance=this._r3Injector.get(this.ngModuleType)}get injector(){return this._r3Injector}destroy(){let t=this._r3Injector;!t.destroyed&&t.destroy(),this.destroyCbs.forEach(e=>e()),this.destroyCbs=null}onDestroy(t){this.destroyCbs.push(t)}},ld=class extends hw{moduleType;constructor(t){super(),this.moduleType=t}create(t){return new ia(this.moduleType,t,[])}};function fw(n,t,e){return new ia(n,t,e,!1)}var cd=class extends jn{injector;componentFactoryResolver=new od(this);instance=null;constructor(t){super();let e=new yr([...t.providers,{provide:jn,useValue:this},{provide:ua,useValue:this.componentFactoryResolver}],t.parent||yo(),t.debugName,new Set(["environment"]));this.injector=e,t.runEnvironmentInitializers&&e.resolveInjectorInitializers()}destroy(){this.injector.destroy()}onDestroy(t){this.injector.onDestroy(t)}};function pw(n,t,e=null){return new cd({providers:n,parent:t,debugName:e,runEnvironmentInitializers:!0}).injector}var GT=(()=>{class n{_injector;cachedInjectors=new Map;constructor(e){this._injector=e}getOrCreateStandaloneInjector(e){if(!e.standalone)return null;if(!this.cachedInjectors.has(e)){let i=Eh(!1,e.type),r=i.length>0?pw([i],this._injector,""):null;this.cachedInjectors.set(e,r)}return this.cachedInjectors.get(e)}ngOnDestroy(){try{for(let e of this.cachedInjectors.values())e!==null&&e.destroy()}finally{this.cachedInjectors.clear()}}static \u0275prov=w({token:n,providedIn:"environment",factory:()=>new n(B(Ze))})}return n})();function A(n){return sa(()=>{let t=gw(n),e=be(M({},t),{decls:n.decls,vars:n.vars,template:n.template,consts:n.consts||null,ngContentSelectors:n.ngContentSelectors,onPush:n.changeDetection===Jf.OnPush,directiveDefs:null,pipeDefs:null,dependencies:t.standalone&&n.dependencies||null,getStandaloneInjector:t.standalone?r=>r.get(GT).getOrCreateStandaloneInjector(e):null,getExternalStyles:null,signals:n.signals??!1,data:n.data||{},encapsulation:n.encapsulation||bn.Emulated,styles:n.styles||vt,_:null,schemas:n.schemas||null,tView:null,id:""});t.standalone&&ji("NgStandalone"),_w(e);let i=n.dependencies;return e.directiveDefs=xy(i,WT),e.pipeDefs=xy(i,db),e.id=KT(e),e})}function WT(n){return Ii(n)||vh(n)}function I(n){return sa(()=>({type:n.type,bootstrap:n.bootstrap||vt,declarations:n.declarations||vt,imports:n.imports||vt,exports:n.exports||vt,transitiveCompileScopes:null,schemas:n.schemas||null,id:n.id||null}))}function qT(n,t){if(n==null)return Mi;let e={};for(let i in n)if(n.hasOwnProperty(i)){let r=n[i],o,s,a,l;Array.isArray(r)?(a=r[0],o=r[1],s=r[2]??o,l=r[3]||null):(o=r,s=r,a=_d.None,l=null),e[o]=[i,a,l],t[o]=s}return e}function QT(n){if(n==null)return Mi;let t={};for(let e in n)n.hasOwnProperty(e)&&(t[n[e]]=e);return t}function T(n){return sa(()=>{let t=gw(n);return _w(t),t})}function gw(n){let t={};return{type:n.type,providersResolver:null,viewProvidersResolver:null,factory:null,hostBindings:n.hostBindings||null,hostVars:n.hostVars||0,hostAttrs:n.hostAttrs||null,contentQueries:n.contentQueries||null,declaredInputs:t,inputConfig:n.inputs||Mi,exportAs:n.exportAs||null,standalone:n.standalone??!0,signals:n.signals===!0,selectors:n.selectors||vt,viewQuery:n.viewQuery||null,features:n.features||null,setInput:null,resolveHostDirectives:null,hostDirectives:null,controlDef:null,inputs:qT(n.inputs,t),outputs:QT(n.outputs),debugInfo:null}}function _w(n){n.features?.forEach(t=>t(n))}function xy(n,t){return n?()=>{let e=typeof n=="function"?n():n,i=[];for(let r of e){let o=t(r);o!==null&&i.push(o)}return i}:null}function KT(n){let t=0,e=typeof n.consts=="function"?"":n.consts,i=[n.selectors,n.ngContentSelectors,n.hostVars,n.hostAttrs,e,n.vars,n.decls,n.encapsulation,n.standalone,n.signals,n.exportAs,JSON.stringify(n.inputs),JSON.stringify(n.outputs),Object.getOwnPropertyNames(n.type.prototype),!!n.contentQueries,!!n.viewQuery];for(let o of i.join("|"))t=Math.imul(31,t)+o.charCodeAt(0)<<0;return t+=2147483648,"c"+t}function YT(n){return Object.getPrototypeOf(n.prototype).constructor}function re(n){let t=YT(n.type),e=!0,i=[n];for(;t&&t!==Function.prototype&&t!==Object.prototype;){let r,o=Object.hasOwn(t,Vs)?t[Vs]:void 0,s=Object.hasOwn(t,js)?t[js]:void 0;if(Ln(n))r=o??s;else{if(o)throw new D(903,!1);r=s}if(r){if(e){i.push(r);let l=n;l.inputs=lf(n.inputs),l.declaredInputs=lf(n.declaredInputs),l.outputs=lf(n.outputs);let c=r.hostBindings;c&&tA(n,c);let d=r.viewQuery,m=r.contentQueries;if(d&&JT(n,d),m&&eA(n,m),ZT(n,r),lb(n.outputs,r.outputs),Ln(r)&&r.data.animation){let f=n.data;f.animation=(f.animation||[]).concat(r.data.animation)}}let a=r.features;if(a)for(let l=0;l<a.length;l++){let c=a[l];c&&c.ngInherit&&c(n),c===re&&(e=!1)}}t=Object.getPrototypeOf(t)}XT(i)}function ZT(n,t){for(let e in t.inputs){if(!t.inputs.hasOwnProperty(e)||n.inputs.hasOwnProperty(e))continue;let i=t.inputs[e];i!==void 0&&(n.inputs[e]=i,n.declaredInputs[e]=t.declaredInputs[e])}}function XT(n){let t=0,e=null;for(let i=n.length-1;i>=0;i--){let r=n[i];r.hostVars=t+=r.hostVars,r.hostAttrs=Ao(r.hostAttrs,e=Ao(e,r.hostAttrs))}}function lf(n){return n===Mi?{}:n===vt?[]:n}function JT(n,t){let e=n.viewQuery;e?n.viewQuery=(i,r)=>{t(i,r),e(i,r)}:n.viewQuery=t}function eA(n,t){let e=n.contentQueries;e?n.contentQueries=(i,r,o)=>{t(i,r,o),e(i,r,o)}:n.contentQueries=t}function tA(n,t){let e=n.hostBindings;e?n.hostBindings=(i,r)=>{t(i,r),e(i,r)}:n.hostBindings=t}function vw(n,t,e,i,r,o,s,a){if(e.firstCreatePass){n.mergedAttrs=Ao(n.mergedAttrs,n.attrs);let d=n.tView=cp(2,n,r,o,s,e.directiveRegistry,e.pipeRegistry,null,e.schemas,e.consts,null);e.queries!==null&&(e.queries.template(e,n),d.queries=e.queries.embeddedTView(n))}a&&(n.flags|=a),ko(n,!1);let l=iA(e,t,n,i);zc()&&gp(e,t,l,n),Ro(l,t);let c=H0(l,t,l,n);t[i+Ge]=c,up(t,c),NT(c,n,t)}function nA(n,t,e,i,r,o,s,a,l,c,d){let m=e+Ge,f;return t.firstCreatePass?(f=Lo(t,m,4,s||null,a||null),Pc()&&K0(t,n,f,nn(t.consts,c),vp),Py(t,f)):f=t.data[m],vw(f,n,t,e,i,r,o,l),xo(f)&&Cd(t,n,f),c!=null&&la(n,f,d),f}function ra(n,t,e,i,r,o,s,a,l,c,d){let m=e+Ge,f;if(t.firstCreatePass){if(f=Lo(t,m,4,s||null,a||null),c!=null){let h=nn(t.consts,c);f.localNames=[];for(let p=0;p<h.length;p+=2)f.localNames.push(h[p],-1)}}else f=t.data[m];return vw(f,n,t,e,i,r,o,l),c!=null&&la(n,f,d),f}function fe(n,t,e,i,r,o,s,a){let l=ne(),c=Ne(),d=nn(c.consts,o);return nA(l,c,n,t,e,i,r,d,void 0,s,a),fe}var iA=rA;function rA(n,t,e,i){return qs(!0),t[Se].createComment("")}function Hn(n){return typeof n=="function"&&n[lt]!==void 0}function Ap(n){return Hn(n)&&typeof n.set=="function"}var kd=new v(""),Sd=new v(""),ma=(()=>{class n{_ngZone;registry;_isZoneStable=!0;_callbacks=[];_taskTrackingZone=null;_destroyRef;constructor(e,i,r){this._ngZone=e,this.registry=i,Ih()&&(this._destroyRef=u(gn,{optional:!0})??void 0),Rp||(yw(r),r.addToWindow(i)),this._watchAngularEvents(),e.run(()=>{this._taskTrackingZone=typeof Zone>"u"?null:Zone.current.get("TaskTrackingZone")})}_watchAngularEvents(){let e=this._ngZone.onUnstable.subscribe({next:()=>{this._isZoneStable=!1}}),i=this._ngZone.runOutsideAngular(()=>this._ngZone.onStable.subscribe({next:()=>{F.assertNotInAngularZone(),queueMicrotask(()=>{this._isZoneStable=!0,this._runCallbacksIfReady()})}}));this._destroyRef?.onDestroy(()=>{e.unsubscribe(),i.unsubscribe()})}isStable(){return this._isZoneStable&&!this._ngZone.hasPendingMacrotasks}_runCallbacksIfReady(){if(this.isStable())queueMicrotask(()=>{for(;this._callbacks.length!==0;){let e=this._callbacks.pop();clearTimeout(e.timeoutId),e.doneCb()}});else{let e=this.getPendingTasks();this._callbacks=this._callbacks.filter(i=>i.updateCb&&i.updateCb(e)?(clearTimeout(i.timeoutId),!1):!0)}}getPendingTasks(){return this._taskTrackingZone?this._taskTrackingZone.macroTasks.map(e=>({source:e.source,creationLocation:e.creationLocation,data:e.data})):[]}addCallback(e,i,r){let o=-1;i&&i>0&&(o=setTimeout(()=>{this._callbacks=this._callbacks.filter(s=>s.timeoutId!==o),e()},i)),this._callbacks.push({doneCb:e,timeoutId:o,updateCb:r})}whenStable(e,i,r){if(r&&!this._taskTrackingZone)throw new Error('Task tracking zone is required when passing an update callback to whenStable(). Is "zone.js/plugins/task-tracking" loaded?');this.addCallback(e,i,r),this._runCallbacksIfReady()}registerApplication(e){this.registry.registerApplication(e,this)}unregisterApplication(e){this.registry.unregisterApplication(e)}findProviders(e,i,r){return[]}static \u0275fac=function(i){return new(i||n)(B(F),B(bw),B(Sd))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),bw=(()=>{class n{_applications=new Map;registerApplication(e,i){this._applications.set(e,i)}unregisterApplication(e){this._applications.delete(e)}unregisterAllApplications(){this._applications.clear()}getTestability(e){return this._applications.get(e)||null}getAllTestabilities(){return Array.from(this._applications.values())}getAllRootElements(){return Array.from(this._applications.keys())}findTestabilityInTree(e,i=!0){return Rp?.findTestabilityInTree(this,e,i)??null}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})();function yw(n){Rp=n}var Rp;function Bo(n){return!!n&&typeof n.then=="function"}function Op(n){return!!n&&typeof n.subscribe=="function"}var ww=new v("");var Np=(()=>{class n{resolve;reject;initialized=!1;done=!1;donePromise=new Promise((e,i)=>{this.resolve=e,this.reject=i});appInits=u(ww,{optional:!0})??[];injector=u($);constructor(){}runInitializers(){if(this.initialized)return;let e=[];for(let r of this.appInits){let o=wo(this.injector,r);if(Bo(o))e.push(o);else if(Op(o)){let s=new Promise((a,l)=>{o.subscribe({complete:a,error:l})});e.push(s)}}let i=()=>{this.done=!0,this.resolve()};Promise.all(e).then(()=>{i()}).catch(r=>{this.reject(r)}),e.length===0&&i(),this.initialized=!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Cw=new v("");function Dw(){Pm(()=>{let n="";throw new D(600,n)})}function xw(n){return n.isBoundToModule}var oA=10;function Fp(n,t){return Array.isArray(t)?t.reduce(Fp,n):M(M({},n),t)}var Ut=(()=>{class n{_runningTick=!1;_destroyed=!1;_destroyListeners=[];_views=[];internalErrorHandler=u(Ni);afterRenderManager=u(bd);zonelessEnabled=u(Qs);rootEffectScheduler=u(Hc);dirtyFlags=0;tracingSnapshot=null;allTestViews=new Set;autoDetectTestViews=new Set;includeAllTestViews=!1;afterTick=new k;get allViews(){return[...(this.includeAllTestViews?this.allTestViews:this.autoDetectTestViews).keys(),...this._views]}get destroyed(){return this._destroyed}componentTypes=[];components=[];internalPendingTask=u(Mr);get isStable(){return this.internalPendingTask.hasPendingTasksObservable.pipe(ye(e=>!e))}constructor(){u(yn,{optional:!0})}whenStable(){let e;return new Promise(i=>{e=this.isStable.subscribe({next:r=>{r&&i()}})}).finally(()=>{e.unsubscribe()})}_injector=u(Ze);_rendererFactory=null;get injector(){return this._injector}bootstrap(e,i){return this.bootstrapImpl(e,i)}bootstrapImpl(e,i,r=$.NULL){return this._injector.get(F).run(()=>{Me(De.BootstrapComponentStart);let s=e instanceof Ed;if(!this._injector.get(Np).done){let p="";throw new D(405,p)}let l;s?l=e:l=this._injector.get(ua).resolveComponentFactory(e),this.componentTypes.push(l.componentType);let c=xw(l)?void 0:this._injector.get(jn),d=i||l.selector,m=l.create(r,[],d,c),f=m.location.nativeElement,h=m.injector.get(kd,null);return h?.registerApplication(f),m.onDestroy(()=>{this.detachView(m.hostView),Xs(this.components,m),h?.unregisterApplication(f)}),this._loadComponent(m),Me(De.BootstrapComponentEnd,m),m})}tick(){this.zonelessEnabled||(this.dirtyFlags|=1),this._tick()}_tick(){Me(De.ChangeDetectionStart),this.tracingSnapshot!==null?this.tracingSnapshot.run(vd.CHANGE_DETECTION,this.tickImpl):this.tickImpl()}tickImpl=()=>{if(this._runningTick)throw Me(De.ChangeDetectionEnd),new D(101,!1);let e=K(null);try{this._runningTick=!0,this.synchronize()}finally{this._runningTick=!1,this.tracingSnapshot?.dispose(),this.tracingSnapshot=null,K(e),this.afterTick.next(),Me(De.ChangeDetectionEnd)}};synchronize(){this._rendererFactory===null&&!this._injector.destroyed&&(this._rendererFactory=this._injector.get(qe,null,{optional:!0}));let e=0;for(;this.dirtyFlags!==0&&e++<oA;){Me(De.ChangeDetectionSyncStart);try{this.synchronizeOnce()}finally{Me(De.ChangeDetectionSyncEnd)}}}synchronizeOnce(){this.dirtyFlags&16&&(this.dirtyFlags&=-17,this.rootEffectScheduler.flush());let e=!1;if(this.dirtyFlags&7){let i=!!(this.dirtyFlags&1);this.dirtyFlags&=-8,this.dirtyFlags|=8;for(let{_lView:r}of this.allViews){if(!i&&!Gs(r))continue;let o=i&&!this.zonelessEnabled?0:1;B0(r,o),e=!0}if(this.dirtyFlags&=-5,this.syncDirtyFlagsWithViews(),this.dirtyFlags&23)return}e||(this._rendererFactory?.begin?.(),this._rendererFactory?.end?.()),this.dirtyFlags&8&&(this.dirtyFlags&=-9,this.afterRenderManager.execute()),this.syncDirtyFlagsWithViews()}syncDirtyFlagsWithViews(){if(this.allViews.some(({_lView:e})=>Gs(e))){this.dirtyFlags|=2;return}else this.dirtyFlags&=-8}attachView(e){let i=e;this._views.push(i),i.attachToAppRef(this)}detachView(e){let i=e;Xs(this._views,i),i.detachFromAppRef()}_loadComponent(e){this.attachView(e.hostView);try{this.tick()}catch(r){this.internalErrorHandler(r)}this.components.push(e),this._injector.get(Cw,[]).forEach(r=>r(e))}ngOnDestroy(){if(!this._destroyed)try{this._destroyListeners.forEach(e=>e()),this._views.slice().forEach(e=>e.destroy())}finally{this._destroyed=!0,this._views=[],this._destroyListeners=[]}}onDestroy(e){return this._destroyListeners.push(e),()=>Xs(this._destroyListeners,e)}destroy(){if(this._destroyed)throw new D(406,!1);let e=this._injector;e.destroy&&!e.destroyed&&e.destroy()}get viewCount(){return this._views.length}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Xs(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function Id(n,t){let e=ne(),i=ei();if(zn(e,i,t)){let r=Ne(),o=So();if(Dd(o,r,e,n,t))Pn(o)&&A0(e,o.index);else{let a=en(o,e);R0(e[Se],a,null,o.value,n,t,null)}}return Id}function J(n,t,e,i){let r=ne(),o=ei();if(zn(r,o,t)){let s=Ne(),a=So();OM(a,r,n,t,e,i)}return J}var zf=class{destroy(t){}updateValue(t,e){}swap(t,e){let i=Math.min(t,e),r=Math.max(t,e),o=this.detach(r);if(r-i>1){let s=this.detach(i);this.attach(i,o),this.attach(r,s)}else this.attach(i,o)}move(t,e){this.attach(e,this.detach(t))}};function cf(n,t,e,i,r){return n===e&&Object.is(t,i)?1:Object.is(r(n,t),r(e,i))?-1:0}function sA(n,t,e,i){let r,o,s=0,a=n.length-1,l=void 0;if(Array.isArray(t)){K(i);let c=t.length-1;for(K(null);s<=a&&s<=c;){let d=n.at(s),m=t[s],f=cf(s,d,s,m,e);if(f!==0){f<0&&n.updateValue(s,m),s++;continue}let h=n.at(a),p=t[c],b=cf(a,h,c,p,e);if(b!==0){b<0&&n.updateValue(a,p),a--,c--;continue}let C=e(s,d),x=e(a,h),ee=e(s,m);if(Object.is(ee,x)){let ke=e(c,p);Object.is(ke,C)?(n.swap(s,a),n.updateValue(a,p),c--,a--):n.move(a,s),n.updateValue(s,m),s++;continue}if(r??=new dd,o??=ky(n,s,a,e),Hf(n,r,s,ee))n.updateValue(s,m),s++,a++;else if(o.has(ee))r.set(C,n.detach(s)),a--;else{let ke=n.create(s,t[s]);n.attach(s,ke),s++,a++}}for(;s<=c;)Ey(n,r,e,s,t[s]),s++}else if(t!=null){K(i);let c=t[Symbol.iterator]();K(null);let d=c.next();for(;!d.done&&s<=a;){let m=n.at(s),f=d.value,h=cf(s,m,s,f,e);if(h!==0)h<0&&n.updateValue(s,f),s++,d=c.next();else{r??=new dd,o??=ky(n,s,a,e);let p=e(s,f);if(Hf(n,r,s,p))n.updateValue(s,f),s++,a++,d=c.next();else if(!o.has(p))n.attach(s,n.create(s,f)),s++,a++,d=c.next();else{let b=e(s,m);r.set(b,n.detach(s)),a--}}}for(;!d.done;)Ey(n,r,e,n.length,d.value),d=c.next()}for(;s<=a;)n.destroy(n.detach(a--));r?.forEach(c=>{n.destroy(c)})}function Hf(n,t,e,i){return t!==void 0&&t.has(i)?(n.attach(e,t.get(i)),t.delete(i),!0):!1}function Ey(n,t,e,i,r){if(Hf(n,t,i,e(i,r)))n.updateValue(i,r);else{let o=n.create(i,r);n.attach(i,o)}}function ky(n,t,e,i){let r=new Set;for(let o=t;o<=e;o++)r.add(i(o,n.at(o)));return r}var dd=class{kvMap=new Map;_vMap=void 0;has(t){return this.kvMap.has(t)}delete(t){if(!this.has(t))return!1;let e=this.kvMap.get(t);return this._vMap!==void 0&&this._vMap.has(e)?(this.kvMap.set(t,this._vMap.get(e)),this._vMap.delete(e)):this.kvMap.delete(t),!0}get(t){return this.kvMap.get(t)}set(t,e){if(this.kvMap.has(t)){let i=this.kvMap.get(t);this._vMap===void 0&&(this._vMap=new Map);let r=this._vMap;for(;r.has(i);)i=r.get(i);r.set(i,e)}else this.kvMap.set(t,e)}forEach(t){for(let[e,i]of this.kvMap)if(t(i,e),this._vMap!==void 0){let r=this._vMap;for(;r.has(i);)i=r.get(i),t(i,e)}}};function ce(n,t,e,i,r,o,s,a){ji("NgControlFlow");let l=ne(),c=Ne(),d=nn(c.consts,o);return ra(l,c,n,t,e,i,r,d,256,s,a),Pp}function Pp(n,t,e,i,r,o,s,a){ji("NgControlFlow");let l=ne(),c=Ne(),d=nn(c.consts,o);return ra(l,c,n,t,e,i,r,d,512,s,a),Pp}function de(n,t){ji("NgControlFlow");let e=ne(),i=ei(),r=e[i]!==rn?e[i]:-1,o=r!==-1?ud(e,Ge+r):void 0,s=0;if(zn(e,i,n)){let a=K(null);try{if(o!==void 0&&$0(o,s),n!==-1){let l=Ge+n,c=ud(e,l),d=Wf(e[Y],l),m=W0(c,d,e),f=ca(e,d,t,{dehydratedView:m});da(c,f,s,Oo(d,m))}}finally{K(a)}}else if(o!==void 0){let a=U0(o,s);a!==void 0&&(a[Xe]=t)}}var Uf=class{lContainer;$implicit;$index;constructor(t,e,i){this.lContainer=t,this.$implicit=e,this.$index=i}get $count(){return this.lContainer.length-We}};function Lp(n,t){return t}var $f=class{hasEmptyBlock;trackByFn;liveCollection;constructor(t,e,i){this.hasEmptyBlock=t,this.trackByFn=e,this.liveCollection=i}};function Bp(n,t,e,i,r,o,s,a,l,c,d,m,f){ji("NgControlFlow");let h=ne(),p=Ne(),b=l!==void 0,C=ne(),x=a?s.bind(C[Et][Xe]):s,ee=new $f(b,x);C[Ge+n]=ee,ra(h,p,n+1,t,e,i,r,nn(p.consts,o),256),b&&ra(h,p,n+2,l,c,d,m,nn(p.consts,f),512)}var Gf=class extends zf{lContainer;hostLView;templateTNode;operationsCounter=void 0;needsIndexUpdate=!1;constructor(t,e,i){super(),this.lContainer=t,this.hostLView=e,this.templateTNode=i}get length(){return this.lContainer.length-We}at(t){return this.getLView(t)[Xe].$implicit}attach(t,e){let i=e[Cr];this.needsIndexUpdate||=t!==this.length,da(this.lContainer,e,t,Oo(this.templateTNode,i)),aA(this.lContainer,t)}detach(t){return this.needsIndexUpdate||=t!==this.length-1,lA(this.lContainer,t),cA(this.lContainer,t)}create(t,e){let i=id(this.lContainer,this.templateTNode.tView.ssrId);return ca(this.hostLView,this.templateTNode,new Uf(this.lContainer,e,t),{dehydratedView:i})}destroy(t){yd(t[Y],t)}updateValue(t,e){this.getLView(t)[Xe].$implicit=e}reset(){this.needsIndexUpdate=!1}updateIndexes(){if(this.needsIndexUpdate)for(let t=0;t<this.length;t++)this.getLView(t)[Xe].$index=t}getLView(t){return dA(this.lContainer,t)}};function Vp(n){let t=K(null),e=ti();try{let i=ne(),r=i[Y],o=i[e],s=e+1,a=ud(i,s);if(o.liveCollection===void 0){let c=Wf(r,s);o.liveCollection=new Gf(a,i,c)}else o.liveCollection.reset();let l=o.liveCollection;if(sA(l,n,o.trackByFn,t),l.updateIndexes(),o.hasEmptyBlock){let c=ei(),d=l.length===0;if(zn(i,c,d)){let m=e+2,f=ud(i,m);if(d){let h=Wf(r,m),p=W0(f,h,i),b=ca(i,h,void 0,{dehydratedView:p});da(f,b,0,Oo(h,p))}else r.firstUpdatePass&&nT(f),$0(f,0)}}}finally{K(t)}}function ud(n,t){return n[t]}function aA(n,t){if(n.length<=We)return;let e=We+t,i=n[e],r=i?i[Ai]:void 0;if(i&&r&&r.detachedLeaveAnimationFns&&r.detachedLeaveAnimationFns.length>0){let o=i[Zn];mM(o,r),Rr.delete(i[Xn]),r.detachedLeaveAnimationFns=void 0}}function lA(n,t){if(n.length<=We)return;let e=We+t,i=n[e],r=i?i[Ai]:void 0;r&&r.leave&&r.leave.size>0&&(r.detachedLeaveAnimationFns=[])}function cA(n,t){return na(n,t)}function dA(n,t){return U0(n,t)}function Wf(n,t){return Oc(n,t)}function R(n,t,e){let i=ne(),r=ei();if(zn(i,r,t)){let o=Ne(),s=So();M0(s,i,n,t,i[Se],e)}return R}function qf(n,t,e,i,r){Dd(t,n,e,r?"class":"style",i)}function _(n,t,e,i){let r=ne(),o=r[Y],s=n+Ge,a=o.firstCreatePass?Dp(s,r,2,t,vp,Pc(),e,i):o.data[s];if(Pn(a)){let l=r[Nn].tracingService;if(l&&l.componentCreate){let c=o.data[a.directiveStart+a.componentOffset];return l.componentCreate(tw(c),()=>(Sy(n,t,r,a,i),_))}}return Sy(n,t,r,a,i),_}function Sy(n,t,e,i,r){if(bp(i,e,n,t,Ew),xo(i)){let o=e[Y];Cd(o,e,i),tp(o,i,e)}r!=null&&la(e,i)}function g(){let n=Ne(),t=ot(),e=yp(t);return n.firstCreatePass&&xp(n,e),Hh(e)&&Uh(),jh(),e.classesWithoutHost!=null&&KS(e)&&qf(n,e,ne(),e.classesWithoutHost,!0),e.stylesWithoutHost!=null&&YS(e)&&qf(n,e,ne(),e.stylesWithoutHost,!1),g}function Z(n,t,e,i){return _(n,t,e,i),g(),Z}function Ot(n,t,e,i){let r=ne(),o=r[Y],s=n+Ge,a=o.firstCreatePass?gT(s,o,2,t,e,i):o.data[s];return bp(a,r,n,t,Ew),i!=null&&la(r,a),Ot}function $t(){let n=ot(),t=yp(n);return Hh(t)&&Uh(),jh(),$t}function on(n,t,e,i){return Ot(n,t,e,i),$t(),on}var Ew=(n,t,e,i,r)=>(qs(!0),m0(t[Se],i,Zh()));function wt(n,t,e){let i=ne(),r=i[Y],o=n+Ge,s=r.firstCreatePass?Dp(o,i,8,"ng-container",vp,Pc(),t,e):r.data[o];if(bp(s,i,n,"ng-container",uA),xo(s)){let a=i[Y];Cd(a,i,s),tp(a,s,i)}return e!=null&&la(i,s),wt}function Ct(){let n=Ne(),t=ot(),e=yp(t);return n.firstCreatePass&&xp(n,e),Ct}function nt(n,t,e){return wt(n,t,e),Ct(),nt}var uA=(n,t,e,i,r)=>(qs(!0),HI(t[Se],""));function Je(){return ne()}function Nt(n,t,e){let i=ne(),r=ei();if(zn(i,r,t)){let o=Ne(),s=So();T0(s,i,n,t,i[Se],e)}return Nt}var ha="en-US";var mA=ha;function kw(n){typeof n=="string"&&(mA=n.toLowerCase().replace(/_/g,"-"))}function V(n,t,e){let i=ne(),r=Ne(),o=ot();return Sw(r,i,i[Se],o,n,t,e),V}function Md(n,t,e){let i=ne(),r=Ne(),o=ot();return(o.type&3||e)&&J0(o,r,i,e,i[Se],n,t,Zc(o,i,t)),Md}function Sw(n,t,e,i,r,o,s){let a=!0,l=null;if((i.type&3||s)&&(l??=Zc(i,t,o),J0(i,n,t,s,e,r,o,l)&&(a=!1)),a){let c=i.outputs?.[r],d=i.hostDirectiveOutputs?.[r];if(d&&d.length)for(let m=0;m<d.length;m+=2){let f=d[m],h=d[m+1];l??=Zc(i,t,o),wy(i,t,f,h,r,l)}if(c&&c.length)for(let m of c)l??=Zc(i,t,o),wy(i,t,m,r,r,l)}}function H(n=1){return Ub(n)}function hA(n,t){let e=null,i=ZI(n);for(let r=0;r<t.length;r++){let o=t[r];if(o==="*"){e=r;continue}if(i===null?_0(n,o,!0):eM(i,o))return r}return e}function Ee(n){let t=ne()[Et][xt];if(!t.projection){let e=n?n.length:1,i=t.projection=gb(e,null),r=i.slice(),o=t.child;for(;o!==null;){if(o.type!==128){let s=n?hA(o,n):0;s!==null&&(r[s]?r[s].projectionNext=o:i[s]=o,r[s]=o)}o=o.next}}}function ie(n,t=0,e,i,r,o){let s=ne(),a=Ne(),l=i?n+1:null;l!==null&&ra(s,a,l,i,r,o,null,e);let c=Lo(a,Ge+n,16,null,e||null);c.projection===null&&(c.projection=t),Wh();let m=!s[Cr]||zh();s[Et][xt].projection[c.projection]===null&&l!==null?fA(s,a,l):m&&!fd(c)&&DM(a,s,c)}function fA(n,t,e){let i=Ge+e,r=t.data[i],o=n[i],s=id(o,r.tView.ssrId),a=ca(n,r,void 0,{dehydratedView:s});da(o,a,0,Oo(r,s))}function kt(n,t,e,i){return aw(n,t,e,i),kt}function Te(n,t,e){return sw(n,t,e),Te}function G(n){let t=ne(),e=Ne(),i=Bc();Ws(i+1);let r=Sp(e,i);if(n.dirty&&Eb(t)===((r.metadata.flags&2)===2)){if(r.matches===null)n.reset([]);else{let o=cw(t,i);n.reset(o,Qy),n.notifyOnChanges()}return!0}return!1}function W(){return kp(ne(),Bc())}function Td(n,t,e,i,r){return uw(t,aw(n,e,i,r)),Td}function Ad(n,t,e,i){return uw(n,sw(t,e,i)),Ad}function Rd(n=1){Ws(Bc()+n)}function dt(n){let t=Ob();return xb(t,Ge+n)}function Gc(n,t){return n<<17|t<<2}function Or(n){return n>>17&32767}function pA(n){return(n&2)==2}function gA(n,t){return n&131071|t<<17}function Qf(n){return n|2}function Fo(n){return(n&131068)>>2}function df(n,t){return n&-131069|t<<2}function _A(n){return(n&1)===1}function Kf(n){return n|1}function vA(n,t,e,i,r,o){let s=o?t.classBindings:t.styleBindings,a=Or(s),l=Fo(s);n[i]=e;let c=!1,d;if(Array.isArray(e)){let m=e;d=m[1],(d===null||vo(m,d)>0)&&(c=!0)}else d=e;if(r)if(l!==0){let f=Or(n[a+1]);n[i+1]=Gc(f,a),f!==0&&(n[f+1]=df(n[f+1],i)),n[a+1]=gA(n[a+1],i)}else n[i+1]=Gc(a,0),a!==0&&(n[a+1]=df(n[a+1],i)),a=i;else n[i+1]=Gc(l,0),a===0?a=i:n[l+1]=df(n[l+1],i),l=i;c&&(n[i+1]=Qf(n[i+1])),Iy(n,d,i,!0),Iy(n,d,i,!1),bA(t,d,n,i,o),s=Gc(a,l),o?t.classBindings=s:t.styleBindings=s}function bA(n,t,e,i,r){let o=r?n.residualClasses:n.residualStyles;o!=null&&typeof t=="string"&&vo(o,t)>=0&&(e[i+1]=Kf(e[i+1]))}function Iy(n,t,e,i){let r=n[e+1],o=t===null,s=i?Or(r):Fo(r),a=!1;for(;s!==0&&(a===!1||o);){let l=n[s],c=n[s+1];yA(l,t)&&(a=!0,n[s+1]=i?Kf(c):Qf(c)),s=i?Or(c):Fo(c)}a&&(n[e+1]=i?Qf(r):Kf(r))}function yA(n,t){return n===null||t==null||(Array.isArray(n)?n[1]:n)===t?!0:Array.isArray(n)&&typeof t=="string"?vo(n,t)>=0:!1}var vn={textEnd:0,key:0,keyEnd:0,value:0,valueEnd:0};function wA(n){return n.substring(vn.key,vn.keyEnd)}function CA(n){return DA(n),Iw(n,Mw(n,0,vn.textEnd))}function Iw(n,t){let e=vn.textEnd;return e===t?-1:(t=vn.keyEnd=xA(n,vn.key=t,e),Mw(n,t,e))}function DA(n){vn.key=0,vn.keyEnd=0,vn.value=0,vn.valueEnd=0,vn.textEnd=n.length}function Mw(n,t,e){for(;t<e&&n.charCodeAt(t)<=32;)t++;return t}function xA(n,t,e){for(;t<e&&n.charCodeAt(t)>32;)t++;return t}function Un(n,t,e){return Tw(n,t,e,!1),Un}function j(n,t){return Tw(n,t,null,!0),j}function St(n){kA(RA,EA,n,!0)}function EA(n,t){for(let e=CA(t);e>=0;e=Iw(t,e))Tc(n,wA(t),!0)}function Tw(n,t,e,i){let r=ne(),o=Ne(),s=Qh(2);if(o.firstUpdatePass&&Rw(o,n,s,i),t!==rn&&zn(r,s,t)){let a=o.data[ti()];Ow(o,a,r,r[Se],n,r[s+1]=NA(t,e),i,s)}}function kA(n,t,e,i){let r=Ne(),o=Qh(2);r.firstUpdatePass&&Rw(r,null,o,i);let s=ne();if(e!==rn&&zn(s,o,e)){let a=r.data[ti()];if(Nw(a,i)&&!Aw(r,o)){let l=i?a.classesWithoutHost:a.stylesWithoutHost;l!==null&&(e=xc(l,e||"")),qf(r,a,s,e,i)}else OA(r,a,s,s[Se],s[o+1],s[o+1]=AA(n,t,e),i,o)}}function Aw(n,t){return t>=n.expandoStartIndex}function Rw(n,t,e,i){let r=n.data;if(r[e+1]===null){let o=r[ti()],s=Aw(n,e);Nw(o,i)&&t===null&&!s&&(t=!1),t=SA(r,o,t,i),vA(r,o,t,e,s,i)}}function SA(n,t,e,i){let r=Vb(n),o=i?t.residualClasses:t.residualStyles;if(r===null)(i?t.classBindings:t.styleBindings)===0&&(e=uf(null,n,t,e,i),e=oa(e,t.attrs,i),o=null);else{let s=t.directiveStylingLast;if(s===-1||n[s]!==r)if(e=uf(r,n,t,e,i),o===null){let l=IA(n,t,i);l!==void 0&&Array.isArray(l)&&(l=uf(null,n,t,l[1],i),l=oa(l,t.attrs,i),MA(n,t,i,l))}else o=TA(n,t,i)}return o!==void 0&&(i?t.residualClasses=o:t.residualStyles=o),e}function IA(n,t,e){let i=e?t.classBindings:t.styleBindings;if(Fo(i)!==0)return n[Or(i)]}function MA(n,t,e,i){let r=e?t.classBindings:t.styleBindings;n[Or(r)]=i}function TA(n,t,e){let i,r=t.directiveEnd;for(let o=1+t.directiveStylingLast;o<r;o++){let s=n[o].hostAttrs;i=oa(i,s,e)}return oa(i,t.attrs,e)}function uf(n,t,e,i,r){let o=null,s=e.directiveEnd,a=e.directiveStylingLast;for(a===-1?a=e.directiveStart:a++;a<s&&(o=t[a],i=oa(i,o.hostAttrs,r),o!==n);)a++;return n!==null&&(e.directiveStylingLast=a),i}function oa(n,t,e){let i=e?1:2,r=-1;if(t!==null)for(let o=0;o<t.length;o++){let s=t[o];typeof s=="number"?r=s:r===i&&(Array.isArray(n)||(n=n===void 0?[]:["",n]),Tc(n,s,e?!0:t[++o]))}return n===void 0?null:n}function AA(n,t,e){if(e==null||e==="")return vt;let i=[],r=ri(e);if(Array.isArray(r))for(let o=0;o<r.length;o++)n(i,r[o],!0);else if(r instanceof Set)for(let o of r)n(i,o,!0);else if(typeof r=="object")for(let o in r)r.hasOwnProperty(o)&&n(i,o,r[o]);else typeof r=="string"&&t(i,r);return i}function RA(n,t,e){let i=String(t);i!==""&&!i.includes(" ")&&Tc(n,i,e)}function OA(n,t,e,i,r,o,s,a){r===rn&&(r=vt);let l=0,c=0,d=0<r.length?r[0]:null,m=0<o.length?o[0]:null;for(;d!==null||m!==null;){let f=l<r.length?r[l+1]:void 0,h=c<o.length?o[c+1]:void 0,p=null,b;d===m?(l+=2,c+=2,f!==h&&(p=m,b=h)):m===null||d!==null&&d<m?(l+=2,p=d):(c+=2,p=m,b=h),p!==null&&Ow(n,t,e,i,p,b,s,a),d=l<r.length?r[l]:null,m=c<o.length?o[c]:null}}function Ow(n,t,e,i,r,o,s,a){if(!(t.type&3))return;let l=n.data,c=l[a+1],d=_A(c)?My(l,t,e,r,Fo(c),s):void 0;if(!md(d)){md(o)||pA(c)&&(o=My(l,null,e,r,a,s));let m=Nh(ti(),e);EM(i,s,m,r,o)}}function My(n,t,e,i,r,o){let s=t===null,a;for(;r>0;){let l=n[r],c=Array.isArray(l),d=c?l[1]:l,m=d===null,f=e[r+1];f===rn&&(f=m?vt:void 0);let h=m?Ac(f,i):d===i?f:void 0;if(c&&!md(h)&&(h=Ac(l,i)),md(h)&&(a=h,s))return a;let p=n[r+1];r=s?Or(p):Fo(p)}if(t!==null){let l=o?t.residualClasses:t.residualStyles;l!=null&&(a=Ac(l,i))}return a}function md(n){return n!==void 0}function NA(n,t){return n==null||n===""||(typeof t=="string"?n=n+t:typeof n=="object"&&(n=Bs(ri(n)))),n}function Nw(n,t){return(n.flags&(t?8:16))!==0}function E(n,t=""){let e=ne(),i=Ne(),r=n+Ge,o=i.firstCreatePass?Lo(i,r,1,t,null):i.data[r],s=FA(i,e,o,t);e[r]=s,zc()&&gp(i,e,s,o),ko(o,!1)}var FA=(n,t,e,i)=>(qs(!0),jI(t[Se],i));function PA(n,t,e,i=""){return zn(n,ei(),e)?t+bh(e)+i:rn}function It(n){return Ye("",n),It}function Ye(n,t,e){let i=ne(),r=PA(i,n,t,e);return r!==rn&&LA(i,ti(),r),Ye}function LA(n,t,e){let i=Nh(t,n);zI(n[Se],i,e)}function Od(n,t,e){Ap(t)&&(t=t());let i=ne(),r=ei();if(zn(i,r,t)){let o=Ne(),s=So();M0(s,i,n,t,i[Se],e)}return Od}function jp(n,t){let e=Ap(n);return e&&n.set(t),e}function Nd(n,t){let e=ne(),i=Ne(),r=ot();return Sw(i,e,e[Se],r,n,t),Nd}function Ty(n,t,e){let i=Ne();i.firstCreatePass&&Fw(t,i.data,i.blueprint,Ln(n),e)}function Fw(n,t,e,i,r){if(n=mt(n),Array.isArray(n))for(let o=0;o<n.length;o++)Fw(n[o],t,e,i,r);else{let o=Ne(),s=ne(),a=ot(),l=br(n)?n:mt(n.provide),c=Sh(n),d=a.providerIndexes&1048575,m=a.directiveStart,f=a.providerIndexes>>20;if(br(n)||!n.multi){let h=new Ar(c,r,q,null),p=hf(l,t,r?d:d+f,m);p===-1?(pf(td(a,s),o,l),mf(o,n,t.length),t.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(h),s.push(h)):(e[p]=h,s[p]=h)}else{let h=hf(l,t,d+f,m),p=hf(l,t,d,d+f),b=h>=0&&e[h],C=p>=0&&e[p];if(r&&!C||!r&&!b){pf(td(a,s),o,l);let x=jA(r?VA:BA,e.length,r,i,c,n);!r&&C&&(e[p].providerFactory=x),mf(o,n,t.length,0),t.push(l),a.directiveStart++,a.directiveEnd++,r&&(a.providerIndexes+=1048576),e.push(x),s.push(x)}else{let x=Pw(e[r?p:h],c,!r&&i);mf(o,n,h>-1?h:p,x)}!r&&i&&C&&e[p].componentProviders++}}}function mf(n,t,e,i){let r=br(t),o=wb(t);if(r||o){let l=(o?mt(t.useClass):t).prototype.ngOnDestroy;if(l){let c=n.destroyHooks||(n.destroyHooks=[]);if(!r&&t.multi){let d=c.indexOf(e);d===-1?c.push(e,[i,l]):c[d+1].push(i,l)}else c.push(e,l)}}}function Pw(n,t,e){return e&&n.componentProviders++,n.multi.push(t)-1}function hf(n,t,e,i){for(let r=e;r<i;r++)if(t[r]===n)return r;return-1}function BA(n,t,e,i,r){return Yf(this.multi,[])}function VA(n,t,e,i,r){let o=this.multi,s;if(this.providerFactory){let a=this.providerFactory.componentProviders,l=Js(i,i[Y],this.providerFactory.index,r);s=l.slice(0,a),Yf(o,s);for(let c=a;c<l.length;c++)s.push(l[c])}else s=[],Yf(o,s);return s}function Yf(n,t){for(let e=0;e<n.length;e++){let i=n[e];t.push(i())}return t}function jA(n,t,e,i,r,o){let s=new Ar(n,e,q,null);return s.multi=[],s.index=t,s.componentProviders=0,Pw(s,r,i&&!e),s}function ve(n,t){return e=>{e.providersResolver=(i,r)=>Ty(i,r?r(n):n,!1),t&&(e.viewProvidersResolver=(i,r)=>Ty(i,r?r(t):t,!0))}}function zp(n,t){let e=Nb()+n,i=ne();return i[e]===rn?_T(i,e,t()):vT(i,e)}function Vo(n,t){return xd(n,t)}var Wc=null;function Lw(n){Wc!==null&&(n.defaultEncapsulation!==Wc.defaultEncapsulation||n.preserveWhitespaces!==Wc.preserveWhitespaces)||(Wc=n)}var Bw=new v("");var Vw=(()=>{class n{applicationErrorHandler=u(Ni);appRef=u(Ut);taskService=u(Mr);ngZone=u(F);zonelessEnabled=u(Qs);tracing=u(yn,{optional:!0});zoneIsDefined=typeof Zone<"u"&&!!Zone.root.run;schedulerTickApplyArgs=[{data:{__scheduler_tick__:!0}}];subscriptions=new ge;angularZoneId=this.zoneIsDefined?this.ngZone._inner?.get(Ps):null;scheduleInRootZone=!this.zonelessEnabled&&this.zoneIsDefined&&(u(nf,{optional:!0})??!1);cancelScheduledCallback=null;useMicrotaskScheduler=!1;runningTick=!1;pendingRenderTaskId=null;constructor(){this.subscriptions.add(this.appRef.afterTick.subscribe(()=>{let e=this.taskService.add();if(!this.runningTick&&(this.cleanup(),!this.zonelessEnabled||this.appRef.includeAllTestViews)){this.taskService.remove(e);return}this.switchToMicrotaskScheduler(),this.taskService.remove(e)})),this.subscriptions.add(this.ngZone.onUnstable.subscribe(()=>{this.runningTick||this.cleanup()}))}switchToMicrotaskScheduler(){this.ngZone.runOutsideAngular(()=>{let e=this.taskService.add();this.useMicrotaskScheduler=!0,queueMicrotask(()=>{this.useMicrotaskScheduler=!1,this.taskService.remove(e)})})}notify(e){if(!this.zonelessEnabled&&e===5)return;switch(e){case 0:{this.appRef.dirtyFlags|=2;break}case 3:case 2:case 4:case 5:case 1:{this.appRef.dirtyFlags|=4;break}case 6:{this.appRef.dirtyFlags|=2;break}case 12:{this.appRef.dirtyFlags|=16;break}case 13:{this.appRef.dirtyFlags|=2;break}case 11:break;default:this.appRef.dirtyFlags|=8}if(this.appRef.tracingSnapshot=this.tracing?.snapshot(this.appRef.tracingSnapshot)??null,!this.shouldScheduleTick())return;let i=this.useMicrotaskScheduler?qb:Jh;this.pendingRenderTaskId=this.taskService.add(),this.scheduleInRootZone?this.cancelScheduledCallback=Zone.root.run(()=>i(()=>this.tick())):this.cancelScheduledCallback=this.ngZone.runOutsideAngular(()=>i(()=>this.tick()))}shouldScheduleTick(){return!(this.appRef.destroyed||this.pendingRenderTaskId!==null||this.runningTick||this.appRef._runningTick||!this.zonelessEnabled&&this.zoneIsDefined&&Zone.current.get(Ps+this.angularZoneId))}tick(){if(this.runningTick||this.appRef.destroyed)return;if(this.appRef.dirtyFlags===0){this.cleanup();return}!this.zonelessEnabled&&this.appRef.dirtyFlags&7&&(this.appRef.dirtyFlags|=1);let e=this.taskService.add();try{this.ngZone.run(()=>{this.runningTick=!0,this.appRef._tick()},void 0,this.schedulerTickApplyArgs)}catch(i){this.applicationErrorHandler(i)}finally{this.taskService.remove(e),this.cleanup()}}ngOnDestroy(){this.subscriptions.unsubscribe(),this.cleanup()}cleanup(){if(this.runningTick=!1,this.cancelScheduledCallback?.(),this.cancelScheduledCallback=null,this.pendingRenderTaskId!==null){let e=this.pendingRenderTaskId;this.pendingRenderTaskId=null,this.taskService.remove(e)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function jw(){return[{provide:On,useExisting:Vw},{provide:F,useClass:Ls},{provide:Qs,useValue:!0}]}function zA(){return typeof $localize<"u"&&$localize.locale||ha}var Fd=new v("",{factory:()=>u(Fd,{optional:!0,skipSelf:!0})||zA()});function sn(n){return ib(n)}function wn(n,t){return ys(n,t?.equal)}var an=class{attributeName;constructor(t){this.attributeName=t}__NG_ELEMENT_ID__=()=>Xf(this.attributeName);toString(){return`HostAttributeToken ${this.attributeName}`}};function zw(n,t){return Ip(t)}function eR(n,t){return Mp(t)}var pa=(zw.required=eR,zw);function Hw(n,t){return Ip(t)}function tR(n,t){return Mp(t)}var Qw=(Hw.required=tR,Hw);function nR(n,t,e){let i=new ld(e);return Promise.resolve(i)}function Uw(n){for(let t=n.length-1;t>=0;t--)if(n[t]!==void 0)return n[t]}var Pd=new v(""),iR=new v("");function fa(n){return!n.moduleRef}function rR(n){let t=fa(n)?n.r3Injector:n.moduleRef.injector,e=t.get(F);return e.run(()=>{fa(n)?n.r3Injector.resolveInjectorInitializers():n.moduleRef.resolveInjectorInitializers();let i=t.get(Ni),r;if(e.runOutsideAngular(()=>{r=e.onError.subscribe({next:i})}),fa(n)){let o=()=>t.destroy(),s=n.platformInjector.get(Pd);s.add(o),t.onDestroy(()=>{r.unsubscribe(),s.delete(o)})}else{let o=()=>n.moduleRef.destroy(),s=n.platformInjector.get(Pd);s.add(o),n.moduleRef.onDestroy(()=>{Xs(n.allPlatformModules,n.moduleRef),r.unsubscribe(),s.delete(o)})}return sR(i,e,()=>{let o=t.get(Mr),s=o.add(),a=t.get(Np);return a.runInitializers(),a.donePromise.then(()=>{let l=t.get(Fd,ha);if(kw(l||ha),!t.get(iR,!0))return fa(n)?t.get(Ut):(n.allPlatformModules.push(n.moduleRef),n.moduleRef);if(fa(n)){let d=t.get(Ut);return n.rootComponent!==void 0&&d.bootstrap(n.rootComponent),d}else return Kw?.(n.moduleRef,n.allPlatformModules),n.moduleRef}).finally(()=>{o.remove(s)})})})}var Kw;function $w(){Kw=oR}function oR(n,t){let e=n.injector.get(Ut);if(n._bootstrapComponents.length>0)n._bootstrapComponents.forEach(i=>e.bootstrap(i));else if(n.instance.ngDoBootstrap)n.instance.ngDoBootstrap(e);else throw new D(-403,!1);t.push(n)}function sR(n,t,e){try{let i=e();return Bo(i)?i.catch(r=>{throw t.runOutsideAngular(()=>n(r)),r}):i}catch(i){throw t.runOutsideAngular(()=>n(i)),i}}var Yw=(()=>{class n{_injector;_modules=[];_destroyListeners=[];_destroyed=!1;constructor(e){this._injector=e}bootstrapModuleFactory(e,i){let r=[jw(),...i?.applicationProviders??[],Kb],o=fw(e.moduleType,this.injector,r);return $w(),rR({moduleRef:o,allPlatformModules:this._modules,platformInjector:this.injector})}bootstrapModule(e,i=[]){let r=Fp({},i);return $w(),nR(this.injector,r,e).then(o=>this.bootstrapModuleFactory(o,r))}onDestroy(e){this._destroyListeners.push(e)}get injector(){return this._injector}destroy(){if(this._destroyed)throw new D(404,!1);this._modules.slice().forEach(i=>i.destroy()),this._destroyListeners.forEach(i=>i());let e=this._injector.get(Pd,null);e&&(e.forEach(i=>i()),e.clear()),this._destroyed=!0}get destroyed(){return this._destroyed}static \u0275fac=function(i){return new(i||n)(B($))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"platform"})}return n})(),Qp=null;function aR(n){if(Yp())throw new D(400,!1);Dw(),Qp=n;let t=n.get(Yw);return dR(n),t}function Kp(n,t,e=[]){let i=`Platform: ${t}`,r=new v(i);return(o=[])=>{let s=Yp();if(!s){let a=[...e,...o,{provide:r,useValue:!0}];s=n?.(a)??aR(lR(a,i))}return cR(r)}}function lR(n=[],t){return $.create({name:t,providers:[{provide:Us,useValue:"platform"},{provide:Pd,useValue:new Set([()=>Qp=null])},...n]})}function cR(n){let t=Yp();if(!t)throw new D(-401,!1);return t}function Yp(){return Qp?.get(Yw)??null}function dR(n){let t=n.get(hd,null);wo(n,()=>{t?.forEach(e=>e())})}var uR=1e4;var n6=uR-1e3;var Ae=(()=>{class n{static __NG_ELEMENT_ID__=mR}return n})();function mR(n){return hR(ot(),ne(),(n&16)===16)}function hR(n,t,e){if(Pn(n)&&!e){let i=tn(n.index,t);return new Li(i,i)}else if(n.type&175){let i=t[Et];return new Li(i,t)}return null}var Up=class{supports(t){return Ep(t)}create(t){return new $p(t)}},fR=(n,t)=>t,$p=class{length=0;collection;_linkedRecords=null;_unlinkedRecords=null;_previousItHead=null;_itHead=null;_itTail=null;_additionsHead=null;_additionsTail=null;_movesHead=null;_movesTail=null;_removalsHead=null;_removalsTail=null;_identityChangesHead=null;_identityChangesTail=null;_trackByFn;constructor(t){this._trackByFn=t||fR}forEachItem(t){let e;for(e=this._itHead;e!==null;e=e._next)t(e)}forEachOperation(t){let e=this._itHead,i=this._removalsHead,r=0,o=null;for(;e||i;){let s=!i||e&&e.currentIndex<Gw(i,r,o)?e:i,a=Gw(s,r,o),l=s.currentIndex;if(s===i)r--,i=i._nextRemoved;else if(e=e._next,s.previousIndex==null)r++;else{o||(o=[]);let c=a-r,d=l-r;if(c!=d){for(let f=0;f<c;f++){let h=f<o.length?o[f]:o[f]=0,p=h+f;d<=p&&p<c&&(o[f]=h+1)}let m=s.previousIndex;o[m]=d-c}}a!==l&&t(s,a,l)}}forEachPreviousItem(t){let e;for(e=this._previousItHead;e!==null;e=e._nextPrevious)t(e)}forEachAddedItem(t){let e;for(e=this._additionsHead;e!==null;e=e._nextAdded)t(e)}forEachMovedItem(t){let e;for(e=this._movesHead;e!==null;e=e._nextMoved)t(e)}forEachRemovedItem(t){let e;for(e=this._removalsHead;e!==null;e=e._nextRemoved)t(e)}forEachIdentityChange(t){let e;for(e=this._identityChangesHead;e!==null;e=e._nextIdentityChange)t(e)}diff(t){if(t==null&&(t=[]),!Ep(t))throw new D(900,!1);return this.check(t)?this:null}onDestroy(){}check(t){this._reset();let e=this._itHead,i=!1,r,o,s;if(Array.isArray(t)){this.length=t.length;for(let a=0;a<this.length;a++)o=t[a],s=this._trackByFn(a,o),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,o,s,a),i=!0):(i&&(e=this._verifyReinsertion(e,o,s,a)),Object.is(e.item,o)||this._addIdentityChange(e,o)),e=e._next}else r=0,Z0(t,a=>{s=this._trackByFn(r,a),e===null||!Object.is(e.trackById,s)?(e=this._mismatch(e,a,s,r),i=!0):(i&&(e=this._verifyReinsertion(e,a,s,r)),Object.is(e.item,a)||this._addIdentityChange(e,a)),e=e._next,r++}),this.length=r;return this._truncate(e),this.collection=t,this.isDirty}get isDirty(){return this._additionsHead!==null||this._movesHead!==null||this._removalsHead!==null||this._identityChangesHead!==null}_reset(){if(this.isDirty){let t;for(t=this._previousItHead=this._itHead;t!==null;t=t._next)t._nextPrevious=t._next;for(t=this._additionsHead;t!==null;t=t._nextAdded)t.previousIndex=t.currentIndex;for(this._additionsHead=this._additionsTail=null,t=this._movesHead;t!==null;t=t._nextMoved)t.previousIndex=t.currentIndex;this._movesHead=this._movesTail=null,this._removalsHead=this._removalsTail=null,this._identityChangesHead=this._identityChangesTail=null}}_mismatch(t,e,i,r){let o;return t===null?o=this._itTail:(o=t._prev,this._remove(t)),t=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null),t!==null?(Object.is(t.item,e)||this._addIdentityChange(t,e),this._reinsertAfter(t,o,r)):(t=this._linkedRecords===null?null:this._linkedRecords.get(i,r),t!==null?(Object.is(t.item,e)||this._addIdentityChange(t,e),this._moveAfter(t,o,r)):t=this._addAfter(new Gp(e,i),o,r)),t}_verifyReinsertion(t,e,i,r){let o=this._unlinkedRecords===null?null:this._unlinkedRecords.get(i,null);return o!==null?t=this._reinsertAfter(o,t._prev,r):t.currentIndex!=r&&(t.currentIndex=r,this._addToMoves(t,r)),t}_truncate(t){for(;t!==null;){let e=t._next;this._addToRemovals(this._unlink(t)),t=e}this._unlinkedRecords!==null&&this._unlinkedRecords.clear(),this._additionsTail!==null&&(this._additionsTail._nextAdded=null),this._movesTail!==null&&(this._movesTail._nextMoved=null),this._itTail!==null&&(this._itTail._next=null),this._removalsTail!==null&&(this._removalsTail._nextRemoved=null),this._identityChangesTail!==null&&(this._identityChangesTail._nextIdentityChange=null)}_reinsertAfter(t,e,i){this._unlinkedRecords!==null&&this._unlinkedRecords.remove(t);let r=t._prevRemoved,o=t._nextRemoved;return r===null?this._removalsHead=o:r._nextRemoved=o,o===null?this._removalsTail=r:o._prevRemoved=r,this._insertAfter(t,e,i),this._addToMoves(t,i),t}_moveAfter(t,e,i){return this._unlink(t),this._insertAfter(t,e,i),this._addToMoves(t,i),t}_addAfter(t,e,i){return this._insertAfter(t,e,i),this._additionsTail===null?this._additionsTail=this._additionsHead=t:this._additionsTail=this._additionsTail._nextAdded=t,t}_insertAfter(t,e,i){let r=e===null?this._itHead:e._next;return t._next=r,t._prev=e,r===null?this._itTail=t:r._prev=t,e===null?this._itHead=t:e._next=t,this._linkedRecords===null&&(this._linkedRecords=new Ld),this._linkedRecords.put(t),t.currentIndex=i,t}_remove(t){return this._addToRemovals(this._unlink(t))}_unlink(t){this._linkedRecords!==null&&this._linkedRecords.remove(t);let e=t._prev,i=t._next;return e===null?this._itHead=i:e._next=i,i===null?this._itTail=e:i._prev=e,t}_addToMoves(t,e){return t.previousIndex===e||(this._movesTail===null?this._movesTail=this._movesHead=t:this._movesTail=this._movesTail._nextMoved=t),t}_addToRemovals(t){return this._unlinkedRecords===null&&(this._unlinkedRecords=new Ld),this._unlinkedRecords.put(t),t.currentIndex=null,t._nextRemoved=null,this._removalsTail===null?(this._removalsTail=this._removalsHead=t,t._prevRemoved=null):(t._prevRemoved=this._removalsTail,this._removalsTail=this._removalsTail._nextRemoved=t),t}_addIdentityChange(t,e){return t.item=e,this._identityChangesTail===null?this._identityChangesTail=this._identityChangesHead=t:this._identityChangesTail=this._identityChangesTail._nextIdentityChange=t,t}},Gp=class{item;trackById;currentIndex=null;previousIndex=null;_nextPrevious=null;_prev=null;_next=null;_prevDup=null;_nextDup=null;_prevRemoved=null;_nextRemoved=null;_nextAdded=null;_nextMoved=null;_nextIdentityChange=null;constructor(t,e){this.item=t,this.trackById=e}},Wp=class{_head=null;_tail=null;add(t){this._head===null?(this._head=this._tail=t,t._nextDup=null,t._prevDup=null):(this._tail._nextDup=t,t._prevDup=this._tail,t._nextDup=null,this._tail=t)}get(t,e){let i;for(i=this._head;i!==null;i=i._nextDup)if((e===null||e<=i.currentIndex)&&Object.is(i.trackById,t))return i;return null}remove(t){let e=t._prevDup,i=t._nextDup;return e===null?this._head=i:e._nextDup=i,i===null?this._tail=e:i._prevDup=e,this._head===null}},Ld=class{map=new Map;put(t){let e=t.trackById,i=this.map.get(e);i||(i=new Wp,this.map.set(e,i)),i.add(t)}get(t,e){let i=t,r=this.map.get(i);return r?r.get(t,e):null}remove(t){let e=t.trackById;return this.map.get(e).remove(t)&&this.map.delete(e),t}get isEmpty(){return this.map.size===0}clear(){this.map.clear()}};function Gw(n,t,e){let i=n.previousIndex;if(i===null)return i;let r=0;return e&&i<e.length&&(r=e[i]),i+t+r}function Ww(){return new si([new Up])}var si=(()=>{class n{factories;static \u0275prov=w({token:n,providedIn:"root",factory:Ww});constructor(e){this.factories=e}static create(e,i){if(i!=null){let r=i.factories.slice();e=e.concat(r)}return new n(e)}static extend(e){return{provide:n,useFactory:()=>{let i=u(n,{optional:!0,skipSelf:!0});return n.create(e,i||Ww())}}}find(e){let i=this.factories.find(r=>r.supports(e));if(i!=null)return i;throw new D(901,!1)}}return n})();var Zw=Kp(null,"core",[]),Xw=(()=>{class n{constructor(e){}static \u0275fac=function(i){return new(i||n)(B(Ut))};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})();function N(n){return typeof n=="boolean"?n:n!=null&&n!=="false"}function ht(n,t=NaN){return!isNaN(parseFloat(n))&&!isNaN(Number(n))?Number(n):t}var Hp=Symbol("NOT_SET"),Jw=new Set,pR=be(M({},Ol),{kind:"afterRenderEffectPhase",consumerIsAlwaysLive:!0,consumerAllowSignalWrites:!0,value:Hp,cleanup:null,consumerMarkedDirty(){if(this.sequence.impl.executing){if(this.sequence.lastPhase===null||this.sequence.lastPhase<this.phase)return;this.sequence.erroredOrDestroyed=!0}this.sequence.scheduler.notify(7)},phaseFn(n){if(this.sequence.lastPhase=this.phase,!this.dirty)return this.signal;if(this.dirty=!1,this.value!==Hp&&!io(this))return this.signal;try{for(let r of this.cleanup??Jw)r()}finally{this.cleanup?.clear()}let t=[];n!==void 0&&t.push(n),t.push(this.registerCleanupFn);let e=Ci(this),i;try{i=this.userFn.apply(null,t)}finally{ar(this,e)}return(this.value===Hp||!this.equal(this.value,i))&&(this.value=i,this.version++),this.signal}}),qp=class extends ea{scheduler;lastPhase=null;nodes=[void 0,void 0,void 0,void 0];onDestroyFns=null;constructor(t,e,i,r,o,s=null){super(t,[void 0,void 0,void 0,void 0],i,!1,o.get(gn),s),this.scheduler=r;for(let a of hp){let l=e[a];if(l===void 0)continue;let c=Object.create(pR);c.sequence=this,c.phase=a,c.userFn=l,c.dirty=!0,c.signal=()=>(no(c),c.value),c.signal[lt]=c,c.registerCleanupFn=d=>(c.cleanup??=new Set).add(d),this.nodes[a]=c,this.hooks[a]=d=>c.phaseFn(d)}}afterRun(){super.afterRun(),this.lastPhase=null}destroy(){if(this.onDestroyFns!==null)for(let t of this.onDestroyFns)t();super.destroy();for(let t of this.nodes)if(t)try{for(let e of t.cleanup??Jw)e()}finally{Di(t)}}};function eC(n,t){let e=t?.injector??u($),i=e.get(On),r=e.get(bd),o=e.get(yn,null,{optional:!0});r.impl??=e.get(fp);let s=n;typeof s=="function"&&(s={mixedReadWrite:n});let a=e.get(Io,null,{optional:!0}),l=new qp(r.impl,[s.earlyRead,s.write,s.mixedReadWrite,s.read],a?.view,i,e,o?.snapshot(null));return r.impl.register(l),l}function Bd(n,t){let e=Ii(n),i=t.elementInjector||yo();return new No(e).create(i,t.projectableNodes,t.hostElement,t.environmentInjector,t.directives,t.bindings)}var tC=null;function Gt(){return tC}function Zp(n){tC??=n}var ga=class{},jo=(()=>{class n{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:()=>u(nC),providedIn:"platform"})}return n})();var nC=(()=>{class n extends jo{_location;_history;_doc=u(U);constructor(){super(),this._location=window.location,this._history=window.history}getBaseHrefFromDOM(){return Gt().getBaseHref(this._doc)}onPopState(e){let i=Gt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("popstate",e,!1),()=>i.removeEventListener("popstate",e)}onHashChange(e){let i=Gt().getGlobalEventTarget(this._doc,"window");return i.addEventListener("hashchange",e,!1),()=>i.removeEventListener("hashchange",e)}get href(){return this._location.href}get protocol(){return this._location.protocol}get hostname(){return this._location.hostname}get port(){return this._location.port}get pathname(){return this._location.pathname}get search(){return this._location.search}get hash(){return this._location.hash}set pathname(e){this._location.pathname=e}pushState(e,i,r){this._history.pushState(e,i,r)}replaceState(e,i,r){this._history.replaceState(e,i,r)}forward(){this._history.forward()}back(){this._history.back()}historyGo(e=0){this._history.go(e)}getState(){return this._history.state}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:()=>new n,providedIn:"platform"})}return n})();function oC(n,t){return n?t?n.endsWith("/")?t.startsWith("/")?n+t.slice(1):n+t:t.startsWith("/")?n+t:`${n}/${t}`:n:t}function iC(n){let t=n.search(/#|\?|$/);return n[t-1]==="/"?n.slice(0,t-1)+n.slice(t):n}function zi(n){return n&&n[0]!=="?"?`?${n}`:n}var Vd=(()=>{class n{historyGo(e){throw new Error("")}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:()=>u(_R),providedIn:"root"})}return n})(),gR=new v(""),_R=(()=>{class n extends Vd{_platformLocation;_baseHref;_removeListenerFns=[];constructor(e,i){super(),this._platformLocation=e,this._baseHref=i??this._platformLocation.getBaseHrefFromDOM()??u(U).location?.origin??""}ngOnDestroy(){for(;this._removeListenerFns.length;)this._removeListenerFns.pop()()}onPopState(e){this._removeListenerFns.push(this._platformLocation.onPopState(e),this._platformLocation.onHashChange(e))}getBaseHref(){return this._baseHref}prepareExternalUrl(e){return oC(this._baseHref,e)}path(e=!1){let i=this._platformLocation.pathname+zi(this._platformLocation.search),r=this._platformLocation.hash;return r&&e?`${i}${r}`:i}pushState(e,i,r,o){let s=this.prepareExternalUrl(r+zi(o));this._platformLocation.pushState(e,i,s)}replaceState(e,i,r,o){let s=this.prepareExternalUrl(r+zi(o));this._platformLocation.replaceState(e,i,s)}forward(){this._platformLocation.forward()}back(){this._platformLocation.back()}getState(){return this._platformLocation.getState()}historyGo(e=0){this._platformLocation.historyGo?.(e)}static \u0275fac=function(i){return new(i||n)(B(jo),B(gR,8))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var jd=(()=>{class n{_subject=new k;_basePath;_locationStrategy;_urlChangeListeners=[];_urlChangeSubscription=null;constructor(e){this._locationStrategy=e;let i=this._locationStrategy.getBaseHref();this._basePath=yR(iC(rC(i))),this._locationStrategy.onPopState(r=>{this._subject.next({url:this.path(!0),pop:!0,state:r.state,type:r.type})})}ngOnDestroy(){this._urlChangeSubscription?.unsubscribe(),this._urlChangeListeners=[]}path(e=!1){return this.normalize(this._locationStrategy.path(e))}getState(){return this._locationStrategy.getState()}isCurrentPathEqualTo(e,i=""){return this.path()==this.normalize(e+zi(i))}normalize(e){return n.stripTrailingSlash(bR(this._basePath,rC(e)))}prepareExternalUrl(e){return e&&e[0]!=="/"&&(e="/"+e),this._locationStrategy.prepareExternalUrl(e)}go(e,i="",r=null){this._locationStrategy.pushState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+zi(i)),r)}replaceState(e,i="",r=null){this._locationStrategy.replaceState(r,"",e,i),this._notifyUrlChangeListeners(this.prepareExternalUrl(e+zi(i)),r)}forward(){this._locationStrategy.forward()}back(){this._locationStrategy.back()}historyGo(e=0){this._locationStrategy.historyGo?.(e)}onUrlChange(e){return this._urlChangeListeners.push(e),this._urlChangeSubscription??=this.subscribe(i=>{this._notifyUrlChangeListeners(i.url,i.state)}),()=>{let i=this._urlChangeListeners.indexOf(e);this._urlChangeListeners.splice(i,1),this._urlChangeListeners.length===0&&(this._urlChangeSubscription?.unsubscribe(),this._urlChangeSubscription=null)}}_notifyUrlChangeListeners(e="",i){this._urlChangeListeners.forEach(r=>r(e,i))}subscribe(e,i,r){return this._subject.subscribe({next:e,error:i??void 0,complete:r??void 0})}static normalizeQueryParams=zi;static joinWithSlash=oC;static stripTrailingSlash=iC;static \u0275fac=function(i){return new(i||n)(B(Vd))};static \u0275prov=w({token:n,factory:()=>vR(),providedIn:"root"})}return n})();function vR(){return new jd(B(Vd))}function bR(n,t){if(!n||!t.startsWith(n))return t;let e=t.substring(n.length);return e===""||["/",";","?","#"].includes(e[0])?e:t}function rC(n){return n.replace(/\/index\.html$/,"")}function yR(n){if(new RegExp("^(https?:)?//").test(n)){let[,e]=n.split(/\/\/[^\/]+/);return e}return n}var _a=(()=>{class n{_viewContainer;_context=new zd;_thenTemplateRef=null;_elseTemplateRef=null;_thenViewRef=null;_elseViewRef=null;constructor(e,i){this._viewContainer=e,this._thenTemplateRef=i}set ngIf(e){this._context.$implicit=this._context.ngIf=e,this._updateView()}set ngIfThen(e){sC(e,!1),this._thenTemplateRef=e,this._thenViewRef=null,this._updateView()}set ngIfElse(e){sC(e,!1),this._elseTemplateRef=e,this._elseViewRef=null,this._updateView()}_updateView(){this._context.$implicit?this._thenViewRef||(this._viewContainer.clear(),this._elseViewRef=null,this._thenTemplateRef&&(this._thenViewRef=this._viewContainer.createEmbeddedView(this._thenTemplateRef,this._context))):this._elseViewRef||(this._viewContainer.clear(),this._thenViewRef=null,this._elseTemplateRef&&(this._elseViewRef=this._viewContainer.createEmbeddedView(this._elseTemplateRef,this._context)))}static ngIfUseIfTypeGuard;static ngTemplateGuard_ngIf;static ngTemplateContextGuard(e,i){return!0}static \u0275fac=function(i){return new(i||n)(q(tt),q(Pe))};static \u0275dir=T({type:n,selectors:[["","ngIf",""]],inputs:{ngIf:"ngIf",ngIfThen:"ngIfThen",ngIfElse:"ngIfElse"}})}return n})(),zd=class{$implicit=null;ngIf=null};function sC(n,t){if(n&&!n.createEmbeddedView)throw new D(2020,!1)}var va=(()=>{class n{_viewContainerRef;_viewRef=null;ngTemplateOutletContext=null;ngTemplateOutlet=null;ngTemplateOutletInjector=null;injector=u($);constructor(e){this._viewContainerRef=e}ngOnChanges(e){if(this._shouldRecreateView(e)){let i=this._viewContainerRef;if(this._viewRef&&i.remove(i.indexOf(this._viewRef)),!this.ngTemplateOutlet){this._viewRef=null;return}let r=this._createContextForwardProxy();this._viewRef=i.createEmbeddedView(this.ngTemplateOutlet,r,{injector:this._getInjector()})}}_getInjector(){return this.ngTemplateOutletInjector==="outlet"?this.injector:this.ngTemplateOutletInjector??void 0}_shouldRecreateView(e){return!!e.ngTemplateOutlet||!!e.ngTemplateOutletInjector}_createContextForwardProxy(){return new Proxy({},{set:(e,i,r)=>this.ngTemplateOutletContext?Reflect.set(this.ngTemplateOutletContext,i,r):!1,get:(e,i,r)=>{if(this.ngTemplateOutletContext)return Reflect.get(this.ngTemplateOutletContext,i,r)}})}static \u0275fac=function(i){return new(i||n)(q(tt))};static \u0275dir=T({type:n,selectors:[["","ngTemplateOutlet",""]],inputs:{ngTemplateOutletContext:"ngTemplateOutletContext",ngTemplateOutlet:"ngTemplateOutlet",ngTemplateOutletInjector:"ngTemplateOutletInjector"},features:[Le]})}return n})();var Xp=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})();function ba(n,t){t=encodeURIComponent(t);for(let e of n.split(";")){let i=e.indexOf("="),[r,o]=i==-1?[e,""]:[e.slice(0,i),e.slice(i+1)];if(r.trim()===t)return decodeURIComponent(o)}return null}var Lr=class{};var Jp="browser";function aC(n){return n===Jp}var ya=class{_doc;constructor(t){this._doc=t}manager},Hd=(()=>{class n extends ya{constructor(e){super(e)}supports(e){return!0}addEventListener(e,i,r,o){return e.addEventListener(i,r,o),()=>this.removeEventListener(e,i,r,o)}removeEventListener(e,i,r,o){return e.removeEventListener(i,r,o)}static \u0275fac=function(i){return new(i||n)(B(U))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),Gd=new v(""),ig=(()=>{class n{_zone;_plugins;_eventNameToPlugin=new Map;constructor(e,i){this._zone=i,e.forEach(s=>{s.manager=this});let r=e.filter(s=>!(s instanceof Hd));this._plugins=r.slice().reverse();let o=e.find(s=>s instanceof Hd);o&&this._plugins.push(o)}addEventListener(e,i,r,o){return this._findPluginFor(i).addEventListener(e,i,r,o)}getZone(){return this._zone}_findPluginFor(e){let i=this._eventNameToPlugin.get(e);if(i)return i;if(i=this._plugins.find(o=>o.supports(e)),!i)throw new D(5101,!1);return this._eventNameToPlugin.set(e,i),i}static \u0275fac=function(i){return new(i||n)(B(Gd),B(F))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),eg="ng-app-id";function cC(n){for(let t of n)t.remove()}function dC(n,t){let e=t.createElement("style");return e.textContent=n,e}function CR(n,t,e,i){let r=n.head?.querySelectorAll(`style[${eg}="${t}"],link[${eg}="${t}"]`);if(r)for(let o of r)o.removeAttribute(eg),o instanceof HTMLLinkElement?i.set(o.href.slice(o.href.lastIndexOf("/")+1),{usage:0,elements:[o]}):o.textContent&&e.set(o.textContent,{usage:0,elements:[o]})}function ng(n,t){let e=t.createElement("link");return e.setAttribute("rel","stylesheet"),e.setAttribute("href",n),e}var rg=(()=>{class n{doc;appId;nonce;inline=new Map;external=new Map;hosts=new Set;constructor(e,i,r,o={}){this.doc=e,this.appId=i,this.nonce=r,CR(e,i,this.inline,this.external),this.hosts.add(e.head)}addStyles(e,i){for(let r of e)this.addUsage(r,this.inline,dC);i?.forEach(r=>this.addUsage(r,this.external,ng))}removeStyles(e,i){for(let r of e)this.removeUsage(r,this.inline);i?.forEach(r=>this.removeUsage(r,this.external))}addUsage(e,i,r){let o=i.get(e);o?o.usage++:i.set(e,{usage:1,elements:[...this.hosts].map(s=>this.addElement(s,r(e,this.doc)))})}removeUsage(e,i){let r=i.get(e);r&&(r.usage--,r.usage<=0&&(cC(r.elements),i.delete(e)))}ngOnDestroy(){for(let[,{elements:e}]of[...this.inline,...this.external])cC(e);this.hosts.clear()}addHost(e){this.hosts.add(e);for(let[i,{elements:r}]of this.inline)r.push(this.addElement(e,dC(i,this.doc)));for(let[i,{elements:r}]of this.external)r.push(this.addElement(e,ng(i,this.doc)))}removeHost(e){this.hosts.delete(e)}addElement(e,i){return this.nonce&&i.setAttribute("nonce",this.nonce),e.appendChild(i)}static \u0275fac=function(i){return new(i||n)(B(U),B(Bi),B(Fr,8),B(Nr))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),tg={svg:"http://www.w3.org/2000/svg",xhtml:"http://www.w3.org/1999/xhtml",xlink:"http://www.w3.org/1999/xlink",xml:"http://www.w3.org/XML/1998/namespace",xmlns:"http://www.w3.org/2000/xmlns/",math:"http://www.w3.org/1998/Math/MathML"},og=/%COMP%/g;var mC="%COMP%",DR=`_nghost-${mC}`,xR=`_ngcontent-${mC}`,ER=!0,kR=new v("",{factory:()=>ER});function SR(n){return xR.replace(og,n)}function IR(n){return DR.replace(og,n)}function hC(n,t){return t.map(e=>e.replace(og,n))}var Da=(()=>{class n{eventManager;sharedStylesHost;appId;removeStylesOnCompDestroy;doc;ngZone;nonce;tracingService;rendererByCompId=new Map;defaultRenderer;constructor(e,i,r,o,s,a,l=null,c=null){this.eventManager=e,this.sharedStylesHost=i,this.appId=r,this.removeStylesOnCompDestroy=o,this.doc=s,this.ngZone=a,this.nonce=l,this.tracingService=c,this.defaultRenderer=new wa(e,s,a,this.tracingService)}createRenderer(e,i){if(!e||!i)return this.defaultRenderer;let r=this.getOrCreateRenderer(e,i);return r instanceof $d?r.applyToHost(e):r instanceof Ca&&r.applyStyles(),r}getOrCreateRenderer(e,i){let r=this.rendererByCompId,o=r.get(i.id);if(!o){let s=this.doc,a=this.ngZone,l=this.eventManager,c=this.sharedStylesHost,d=this.removeStylesOnCompDestroy,m=this.tracingService;switch(i.encapsulation){case bn.Emulated:o=new $d(l,c,i,this.appId,d,s,a,m);break;case bn.ShadowDom:return new Ud(l,e,i,s,a,this.nonce,m,c);case bn.ExperimentalIsolatedShadowDom:return new Ud(l,e,i,s,a,this.nonce,m);default:o=new Ca(l,c,i,d,s,a,m);break}r.set(i.id,o)}return o}ngOnDestroy(){this.rendererByCompId.clear()}componentReplaced(e){this.rendererByCompId.delete(e)}static \u0275fac=function(i){return new(i||n)(B(ig),B(rg),B(Bi),B(kR),B(U),B(F),B(Fr),B(yn,8))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),wa=class{eventManager;doc;ngZone;tracingService;data=Object.create(null);throwOnSyntheticProps=!0;constructor(t,e,i,r){this.eventManager=t,this.doc=e,this.ngZone=i,this.tracingService=r}destroy(){}destroyNode=null;createElement(t,e){return e?this.doc.createElementNS(tg[e]||e,t):this.doc.createElement(t)}createComment(t){return this.doc.createComment(t)}createText(t){return this.doc.createTextNode(t)}appendChild(t,e){(uC(t)?t.content:t).appendChild(e)}insertBefore(t,e,i){t&&(uC(t)?t.content:t).insertBefore(e,i)}removeChild(t,e){e.remove()}selectRootElement(t,e){let i=typeof t=="string"?this.doc.querySelector(t):t;if(!i)throw new D(-5104,!1);return e||(i.textContent=""),i}parentNode(t){return t.parentNode}nextSibling(t){return t.nextSibling}setAttribute(t,e,i,r){if(r){e=r+":"+e;let o=tg[r];o?t.setAttributeNS(o,e,i):t.setAttribute(e,i)}else t.setAttribute(e,i)}removeAttribute(t,e,i){if(i){let r=tg[i];r?t.removeAttributeNS(r,e):t.removeAttribute(`${i}:${e}`)}else t.removeAttribute(e)}addClass(t,e){t.classList.add(e)}removeClass(t,e){t.classList.remove(e)}setStyle(t,e,i,r){r&(Vn.DashCase|Vn.Important)?t.style.setProperty(e,i,r&Vn.Important?"important":""):t.style[e]=i}removeStyle(t,e,i){i&Vn.DashCase?t.style.removeProperty(e):t.style[e]=""}setProperty(t,e,i){t!=null&&(t[e]=i)}setValue(t,e){t.nodeValue=e}listen(t,e,i,r){if(typeof t=="string"&&(t=Gt().getGlobalEventTarget(this.doc,t),!t))throw new D(5102,!1);let o=this.decoratePreventDefault(i);return this.tracingService?.wrapEventListener&&(o=this.tracingService.wrapEventListener(t,e,o)),this.eventManager.addEventListener(t,e,o,r)}decoratePreventDefault(t){return e=>{if(e==="__ngUnwrap__")return t;t(e)===!1&&e.preventDefault()}}};function uC(n){return n.tagName==="TEMPLATE"&&n.content!==void 0}var Ud=class extends wa{hostEl;sharedStylesHost;shadowRoot;constructor(t,e,i,r,o,s,a,l){super(t,r,o,a),this.hostEl=e,this.sharedStylesHost=l,this.shadowRoot=e.attachShadow({mode:"open"}),this.sharedStylesHost&&this.sharedStylesHost.addHost(this.shadowRoot);let c=i.styles;c=hC(i.id,c);for(let m of c){let f=document.createElement("style");s&&f.setAttribute("nonce",s),f.textContent=m,this.shadowRoot.appendChild(f)}let d=i.getExternalStyles?.();if(d)for(let m of d){let f=ng(m,r);s&&f.setAttribute("nonce",s),this.shadowRoot.appendChild(f)}}nodeOrShadowRoot(t){return t===this.hostEl?this.shadowRoot:t}appendChild(t,e){return super.appendChild(this.nodeOrShadowRoot(t),e)}insertBefore(t,e,i){return super.insertBefore(this.nodeOrShadowRoot(t),e,i)}removeChild(t,e){return super.removeChild(null,e)}parentNode(t){return this.nodeOrShadowRoot(super.parentNode(this.nodeOrShadowRoot(t)))}destroy(){this.sharedStylesHost&&this.sharedStylesHost.removeHost(this.shadowRoot)}},Ca=class extends wa{sharedStylesHost;removeStylesOnCompDestroy;styles;styleUrls;constructor(t,e,i,r,o,s,a,l){super(t,o,s,a),this.sharedStylesHost=e,this.removeStylesOnCompDestroy=r;let c=i.styles;this.styles=l?hC(l,c):c,this.styleUrls=i.getExternalStyles?.(l)}applyStyles(){this.sharedStylesHost.addStyles(this.styles,this.styleUrls)}destroy(){this.removeStylesOnCompDestroy&&Rr.size===0&&this.sharedStylesHost.removeStyles(this.styles,this.styleUrls)}},$d=class extends Ca{contentAttr;hostAttr;constructor(t,e,i,r,o,s,a,l){let c=r+"-"+i.id;super(t,e,i,o,s,a,l,c),this.contentAttr=SR(c),this.hostAttr=IR(c)}applyToHost(t){this.applyStyles(),this.setAttribute(t,this.hostAttr,"")}createElement(t,e){let i=super.createElement(t,e);return super.setAttribute(i,this.contentAttr,""),i}};var Wd=class n extends ga{supportsDOMEvents=!0;static makeCurrent(){Zp(new n)}onAndCancel(t,e,i,r){return t.addEventListener(e,i,r),()=>{t.removeEventListener(e,i,r)}}dispatchEvent(t,e){t.dispatchEvent(e)}remove(t){t.remove()}createElement(t,e){return e=e||this.getDefaultDocument(),e.createElement(t)}createHtmlDocument(){return document.implementation.createHTMLDocument("fakeTitle")}getDefaultDocument(){return document}isElementNode(t){return t.nodeType===Node.ELEMENT_NODE}isShadowRoot(t){return t instanceof DocumentFragment}getGlobalEventTarget(t,e){return e==="window"?window:e==="document"?t:e==="body"?t.body:null}getBaseHref(t){let e=MR();return e==null?null:TR(e)}resetBaseElement(){xa=null}getUserAgent(){return window.navigator.userAgent}getCookie(t){return ba(document.cookie,t)}},xa=null;function MR(){return xa=xa||document.head.querySelector("base"),xa?xa.getAttribute("href"):null}function TR(n){return new URL(n,document.baseURI).pathname}var qd=class{addToWindow(t){Rt.getAngularTestability=(i,r=!0)=>{let o=t.findTestabilityInTree(i,r);if(o==null)throw new D(5103,!1);return o},Rt.getAllAngularTestabilities=()=>t.getAllTestabilities(),Rt.getAllAngularRootElements=()=>t.getAllRootElements();let e=i=>{let r=Rt.getAllAngularTestabilities(),o=r.length,s=function(){o--,o==0&&i()};r.forEach(a=>{a.whenStable(s)})};Rt.frameworkStabilizers||(Rt.frameworkStabilizers=[]),Rt.frameworkStabilizers.push(e)}findTestabilityInTree(t,e,i){if(e==null)return null;let r=t.getTestability(e);return r??(i?Gt().isShadowRoot(e)?this.findTestabilityInTree(t,e.host,!0):this.findTestabilityInTree(t,e.parentElement,!0):null)}},AR=(()=>{class n{build(){return new XMLHttpRequest}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),fC=["alt","control","meta","shift"],RR={"\b":"Backspace","	":"Tab","\x7F":"Delete","\x1B":"Escape",Del:"Delete",Esc:"Escape",Left:"ArrowLeft",Right:"ArrowRight",Up:"ArrowUp",Down:"ArrowDown",Menu:"ContextMenu",Scroll:"ScrollLock",Win:"OS"},OR={alt:n=>n.altKey,control:n=>n.ctrlKey,meta:n=>n.metaKey,shift:n=>n.shiftKey},pC=(()=>{class n extends ya{constructor(e){super(e)}supports(e){return n.parseEventName(e)!=null}addEventListener(e,i,r,o){let s=n.parseEventName(i),a=n.eventCallback(s.fullKey,r,this.manager.getZone());return this.manager.getZone().runOutsideAngular(()=>Gt().onAndCancel(e,s.domEventName,a,o))}static parseEventName(e){let i=e.toLowerCase().split("."),r=i.shift();if(i.length===0||!(r==="keydown"||r==="keyup"))return null;let o=n._normalizeKey(i.pop()),s="",a=i.indexOf("code");if(a>-1&&(i.splice(a,1),s="code."),fC.forEach(c=>{let d=i.indexOf(c);d>-1&&(i.splice(d,1),s+=c+".")}),s+=o,i.length!=0||o.length===0)return null;let l={};return l.domEventName=r,l.fullKey=s,l}static matchEventFullKeyCode(e,i){let r=RR[e.key]||e.key,o="";return i.indexOf("code.")>-1&&(r=e.code,o="code."),r==null||!r?!1:(r=r.toLowerCase(),r===" "?r="space":r==="."&&(r="dot"),fC.forEach(s=>{if(s!==r){let a=OR[s];a(e)&&(o+=s+".")}}),o+=r,o===i)}static eventCallback(e,i,r){return o=>{n.matchEventFullKeyCode(o,e)&&r.runGuarded(()=>i(o))}}static _normalizeKey(e){return e==="esc"?"escape":e}static \u0275fac=function(i){return new(i||n)(B(U))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})();function NR(){Wd.makeCurrent()}function FR(){return new At}function PR(){return ep(document),document}var LR=[{provide:Nr,useValue:Jp},{provide:hd,useValue:NR,multi:!0},{provide:U,useFactory:PR}],sg=Kp(Zw,"browser",LR);var BR=[{provide:Sd,useClass:qd},{provide:kd,useClass:ma},{provide:ma,useClass:ma}],VR=[{provide:Us,useValue:"root"},{provide:At,useFactory:FR},{provide:Gd,useClass:Hd,multi:!0},{provide:Gd,useClass:pC,multi:!0},Da,rg,ig,{provide:qe,useExisting:Da},{provide:Lr,useClass:AR},[]],Ea=(()=>{class n{constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[...VR,...BR],imports:[Xp,Xw]})}return n})();var Hi=class n{headers;normalizedNames=new Map;lazyInit;lazyUpdate=null;constructor(t){t?typeof t=="string"?this.lazyInit=()=>{this.headers=new Map,t.split(`
`).forEach(e=>{let i=e.indexOf(":");if(i>0){let r=e.slice(0,i),o=e.slice(i+1).trim();this.addHeaderEntry(r,o)}})}:typeof Headers<"u"&&t instanceof Headers?(this.headers=new Map,t.forEach((e,i)=>{this.addHeaderEntry(i,e)})):this.lazyInit=()=>{this.headers=new Map,Object.entries(t).forEach(([e,i])=>{this.setHeaderEntries(e,i)})}:this.headers=new Map}has(t){return this.init(),this.headers.has(t.toLowerCase())}get(t){this.init();let e=this.headers.get(t.toLowerCase());return e&&e.length>0?e[0]:null}keys(){return this.init(),Array.from(this.normalizedNames.values())}getAll(t){return this.init(),this.headers.get(t.toLowerCase())||null}append(t,e){return this.clone({name:t,value:e,op:"a"})}set(t,e){return this.clone({name:t,value:e,op:"s"})}delete(t,e){return this.clone({name:t,value:e,op:"d"})}maybeSetNormalizedName(t,e){this.normalizedNames.has(e)||this.normalizedNames.set(e,t)}init(){this.lazyInit&&(this.lazyInit instanceof n?this.copyFrom(this.lazyInit):this.lazyInit(),this.lazyInit=null,this.lazyUpdate&&(this.lazyUpdate.forEach(t=>this.applyUpdate(t)),this.lazyUpdate=null))}copyFrom(t){t.init();for(let[e,i]of t.headers.entries())this.headers.set(e,i),this.normalizedNames.set(e,t.normalizedNames.get(e))}clone(t){let e=new n;return e.lazyInit=this.lazyInit&&this.lazyInit instanceof n?this.lazyInit:this,e.lazyUpdate=(this.lazyUpdate||[]).concat([t]),e}applyUpdate(t){let e=t.name.toLowerCase();switch(t.op){case"a":case"s":let i=t.value;if(typeof i=="string"&&(i=[i]),i.length===0)return;this.maybeSetNormalizedName(t.name,e);let r=t.op==="a"?(this.headers.get(e)||[]).slice():[];r.push(...i),this.headers.set(e,r);break;case"d":let o=t.value;if(o===void 0)this.headers.delete(e),this.normalizedNames.delete(e);else{let s=Array.isArray(o)?o:[o],a=this.headers.get(e);if(!a)return;a=a.filter(l=>s.indexOf(l)===-1),a.length===0?(this.headers.delete(e),this.normalizedNames.delete(e)):this.headers.set(e,a)}break}}addHeaderEntry(t,e){let i=t.toLowerCase();this.maybeSetNormalizedName(t,i),this.headers.has(i)?this.headers.get(i).push(e):this.headers.set(i,[e])}setHeaderEntries(t,e){let i=(Array.isArray(e)?e:[e]).map(o=>o.toString()),r=t.toLowerCase();this.headers.set(r,i),this.maybeSetNormalizedName(t,r)}forEach(t){this.init(),Array.from(this.normalizedNames.keys()).forEach(e=>t(this.normalizedNames.get(e),this.headers.get(e)))}};var Kd=class{map=new Map;set(t,e){return this.map.set(t,e),this}get(t){return this.map.has(t)||this.map.set(t,t.defaultValue()),this.map.get(t)}delete(t){return this.map.delete(t),this}has(t){return this.map.has(t)}keys(){return this.map.keys()}},Yd=class{encodeKey(t){return gC(t)}encodeValue(t){return gC(t)}decodeKey(t){return decodeURIComponent(t)}decodeValue(t){return decodeURIComponent(t)}};function jR(n,t){let e=new Map;return n.length>0&&n.replace(/^\?/,"").split("&").forEach(r=>{let o=r.indexOf("="),[s,a]=o==-1?[t.decodeKey(r),""]:[t.decodeKey(r.slice(0,o)),t.decodeValue(r.slice(o+1))],l=e.get(s)||[];l.push(a),e.set(s,l)}),e}var zR=/%(\d[a-f0-9])/gi,HR={40:"@","3A":":",24:"$","2C":",","3B":";","3D":"=","3F":"?","2F":"/"};function gC(n){return encodeURIComponent(n).replace(zR,(t,e)=>HR[e]??t)}function Qd(n){return`${n}`}var ai=class n{map;encoder;updates=null;cloneFrom=null;constructor(t={}){if(this.encoder=t.encoder||new Yd,t.fromString){if(t.fromObject)throw new D(2805,!1);this.map=jR(t.fromString,this.encoder)}else t.fromObject?(this.map=new Map,Object.keys(t.fromObject).forEach(e=>{let i=t.fromObject[e],r=Array.isArray(i)?i.map(Qd):[Qd(i)];this.map.set(e,r)})):this.map=null}has(t){return this.init(),this.map.has(t)}get(t){this.init();let e=this.map.get(t);return e?e[0]:null}getAll(t){return this.init(),this.map.get(t)||null}keys(){return this.init(),Array.from(this.map.keys())}append(t,e){return this.clone({param:t,value:e,op:"a"})}appendAll(t){let e=[];return Object.keys(t).forEach(i=>{let r=t[i];Array.isArray(r)?r.forEach(o=>{e.push({param:i,value:o,op:"a"})}):e.push({param:i,value:r,op:"a"})}),this.clone(e)}set(t,e){return this.clone({param:t,value:e,op:"s"})}delete(t,e){return this.clone({param:t,value:e,op:"d"})}toString(){return this.init(),this.keys().map(t=>{let e=this.encoder.encodeKey(t);return this.map.get(t).map(i=>e+"="+this.encoder.encodeValue(i)).join("&")}).filter(t=>t!=="").join("&")}clone(t){let e=new n({encoder:this.encoder});return e.cloneFrom=this.cloneFrom||this,e.updates=(this.updates||[]).concat(t),e}init(){if(this.map===null&&(this.map=new Map),this.cloneFrom!==null){this.cloneFrom.init();for(let[t,e]of this.cloneFrom.map.entries())this.map.set(t,e);this.updates.forEach(t=>{switch(t.op){case"a":case"s":let e=t.op==="a"?(this.map.get(t.param)||[]).slice():[];e.push(Qd(t.value)),this.map.set(t.param,e);break;case"d":if(t.value!==void 0){let i=(this.map.get(t.param)||[]).slice(),r=i.indexOf(Qd(t.value));r!==-1&&i.splice(r,1),i.length>0?this.map.set(t.param,i):this.map.delete(t.param)}else{this.map.delete(t.param);break}}}),this.cloneFrom=this.updates=null}}};function UR(n){switch(n){case"DELETE":case"GET":case"HEAD":case"OPTIONS":case"JSONP":return!1;default:return!0}}function _C(n){return typeof ArrayBuffer<"u"&&n instanceof ArrayBuffer}function vC(n){return typeof Blob<"u"&&n instanceof Blob}function bC(n){return typeof FormData<"u"&&n instanceof FormData}function $R(n){return typeof URLSearchParams<"u"&&n instanceof URLSearchParams}var yC="Content-Type",wC="Accept",DC="text/plain",xC="application/json",GR=`${xC}, ${DC}, */*`,zo=class n{url;body=null;headers;context;reportProgress=!1;withCredentials=!1;credentials;keepalive=!1;cache;priority;mode;redirect;referrer;integrity;referrerPolicy;responseType="json";method;params;urlWithParams;transferCache;timeout;constructor(t,e,i,r){this.url=e,this.method=t.toUpperCase();let o;if(UR(this.method)||r?(this.body=i!==void 0?i:null,o=r):o=i,o){if(this.reportProgress=!!o.reportProgress,this.withCredentials=!!o.withCredentials,this.keepalive=!!o.keepalive,o.responseType&&(this.responseType=o.responseType),o.headers&&(this.headers=o.headers),o.context&&(this.context=o.context),o.params&&(this.params=o.params),o.priority&&(this.priority=o.priority),o.cache&&(this.cache=o.cache),o.credentials&&(this.credentials=o.credentials),typeof o.timeout=="number"){if(o.timeout<1||!Number.isInteger(o.timeout))throw new D(2822,"");this.timeout=o.timeout}o.mode&&(this.mode=o.mode),o.redirect&&(this.redirect=o.redirect),o.integrity&&(this.integrity=o.integrity),o.referrer!==void 0&&(this.referrer=o.referrer),o.referrerPolicy&&(this.referrerPolicy=o.referrerPolicy),this.transferCache=o.transferCache}if(this.headers??=new Hi,this.context??=new Kd,!this.params)this.params=new ai,this.urlWithParams=e;else{let s=this.params.toString();if(s.length===0)this.urlWithParams=e;else{let a=e.indexOf("?"),l=a===-1?"?":a<e.length-1?"&":"";this.urlWithParams=e+l+s}}}serializeBody(){return this.body===null?null:typeof this.body=="string"||_C(this.body)||vC(this.body)||bC(this.body)||$R(this.body)?this.body:this.body instanceof ai?this.body.toString():typeof this.body=="object"||typeof this.body=="boolean"||Array.isArray(this.body)?JSON.stringify(this.body):this.body.toString()}detectContentTypeHeader(){return this.body===null||bC(this.body)?null:vC(this.body)?this.body.type||null:_C(this.body)?null:typeof this.body=="string"?DC:this.body instanceof ai?"application/x-www-form-urlencoded;charset=UTF-8":typeof this.body=="object"||typeof this.body=="number"||typeof this.body=="boolean"?xC:null}clone(t={}){let e=t.method||this.method,i=t.url||this.url,r=t.responseType||this.responseType,o=t.keepalive??this.keepalive,s=t.priority||this.priority,a=t.cache||this.cache,l=t.mode||this.mode,c=t.redirect||this.redirect,d=t.credentials||this.credentials,m=t.referrer??this.referrer,f=t.integrity||this.integrity,h=t.referrerPolicy||this.referrerPolicy,p=t.transferCache??this.transferCache,b=t.timeout??this.timeout,C=t.body!==void 0?t.body:this.body,x=t.withCredentials??this.withCredentials,ee=t.reportProgress??this.reportProgress,ke=t.headers||this.headers,pe=t.params||this.params,qt=t.context??this.context;return t.setHeaders!==void 0&&(ke=Object.keys(t.setHeaders).reduce((pt,Ve)=>pt.set(Ve,t.setHeaders[Ve]),ke)),t.setParams&&(pe=Object.keys(t.setParams).reduce((pt,Ve)=>pt.set(Ve,t.setParams[Ve]),pe)),new n(e,i,C,{params:pe,headers:ke,context:qt,reportProgress:ee,responseType:r,withCredentials:x,transferCache:p,keepalive:o,cache:a,priority:s,timeout:b,mode:l,redirect:c,credentials:d,referrer:m,integrity:f,referrerPolicy:h})}},Br=(function(n){return n[n.Sent=0]="Sent",n[n.UploadProgress=1]="UploadProgress",n[n.ResponseHeader=2]="ResponseHeader",n[n.DownloadProgress=3]="DownloadProgress",n[n.Response=4]="Response",n[n.User=5]="User",n})(Br||{}),Uo=class{headers;status;statusText;url;ok;type;redirected;responseType;constructor(t,e=200,i="OK"){this.headers=t.headers||new Hi,this.status=t.status!==void 0?t.status:e,this.statusText=t.statusText||i,this.url=t.url||null,this.redirected=t.redirected,this.responseType=t.responseType,this.ok=this.status>=200&&this.status<300}},Zd=class n extends Uo{constructor(t={}){super(t)}type=Br.ResponseHeader;clone(t={}){return new n({headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0})}},ka=class n extends Uo{body;constructor(t={}){super(t),this.body=t.body!==void 0?t.body:null}type=Br.Response;clone(t={}){return new n({body:t.body!==void 0?t.body:this.body,headers:t.headers||this.headers,status:t.status!==void 0?t.status:this.status,statusText:t.statusText||this.statusText,url:t.url||this.url||void 0,redirected:t.redirected??this.redirected,responseType:t.responseType??this.responseType})}},Ho=class extends Uo{name="HttpErrorResponse";message;error;ok=!1;constructor(t){super(t,0,"Unknown Error"),this.status>=200&&this.status<300?this.message=`Http failure during parsing for ${t.url||"(unknown url)"}`:this.message=`Http failure response for ${t.url||"(unknown url)"}: ${t.status} ${t.statusText}`,this.error=t.error||null}},WR=200,qR=204;var QR=new v("");var KR=/^\)\]\}',?\n/;var lg=(()=>{class n{xhrFactory;tracingService=u(yn,{optional:!0});constructor(e){this.xhrFactory=e}maybePropagateTrace(e){return this.tracingService?.propagate?this.tracingService.propagate(e):e}handle(e){if(e.method==="JSONP")throw new D(-2800,!1);let i=this.xhrFactory;return $e(null).pipe(fr(()=>new ae(o=>{let s=i.build();if(s.open(e.method,e.urlWithParams),e.withCredentials&&(s.withCredentials=!0),e.headers.forEach((C,x)=>s.setRequestHeader(C,x.join(","))),e.headers.has(wC)||s.setRequestHeader(wC,GR),!e.headers.has(yC)){let C=e.detectContentTypeHeader();C!==null&&s.setRequestHeader(yC,C)}if(e.timeout&&(s.timeout=e.timeout),e.responseType){let C=e.responseType.toLowerCase();s.responseType=C!=="json"?C:"text"}let a=e.serializeBody(),l=null,c=()=>{if(l!==null)return l;let C=s.statusText||"OK",x=new Hi(s.getAllResponseHeaders()),ee=s.responseURL||e.url;return l=new Zd({headers:x,status:s.status,statusText:C,url:ee}),l},d=this.maybePropagateTrace(()=>{let{headers:C,status:x,statusText:ee,url:ke}=c(),pe=null;x!==qR&&(pe=typeof s.response>"u"?s.responseText:s.response),x===0&&(x=pe?WR:0);let qt=x>=200&&x<300;if(e.responseType==="json"&&typeof pe=="string"){let pt=pe;pe=pe.replace(KR,"");try{pe=pe!==""?JSON.parse(pe):null}catch(Ve){pe=pt,qt&&(qt=!1,pe={error:Ve,text:pe})}}qt?(o.next(new ka({body:pe,headers:C,status:x,statusText:ee,url:ke||void 0})),o.complete()):o.error(new Ho({error:pe,headers:C,status:x,statusText:ee,url:ke||void 0}))}),m=this.maybePropagateTrace(C=>{let{url:x}=c(),ee=new Ho({error:C,status:s.status||0,statusText:s.statusText||"Unknown Error",url:x||void 0});o.error(ee)}),f=m;e.timeout&&(f=this.maybePropagateTrace(C=>{let{url:x}=c(),ee=new Ho({error:new DOMException("Request timed out","TimeoutError"),status:s.status||0,statusText:s.statusText||"Request timeout",url:x||void 0});o.error(ee)}));let h=!1,p=this.maybePropagateTrace(C=>{h||(o.next(c()),h=!0);let x={type:Br.DownloadProgress,loaded:C.loaded};C.lengthComputable&&(x.total=C.total),e.responseType==="text"&&s.responseText&&(x.partialText=s.responseText),o.next(x)}),b=this.maybePropagateTrace(C=>{let x={type:Br.UploadProgress,loaded:C.loaded};C.lengthComputable&&(x.total=C.total),o.next(x)});return s.addEventListener("load",d),s.addEventListener("error",m),s.addEventListener("timeout",f),s.addEventListener("abort",m),e.reportProgress&&(s.addEventListener("progress",p),a!==null&&s.upload&&s.upload.addEventListener("progress",b)),s.send(a),o.next({type:Br.Sent}),()=>{s.removeEventListener("error",m),s.removeEventListener("abort",m),s.removeEventListener("load",d),s.removeEventListener("timeout",f),e.reportProgress&&(s.removeEventListener("progress",p),a!==null&&s.upload&&s.upload.removeEventListener("progress",b)),s.readyState!==s.DONE&&s.abort()}})))}static \u0275fac=function(i){return new(i||n)(B(Lr))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),YR=new v("",{factory:()=>!0}),ZR="XSRF-TOKEN",XR=new v("",{factory:()=>ZR}),JR="X-XSRF-TOKEN",e1=new v("",{factory:()=>JR}),t1=(()=>{class n{cookieName=u(XR);doc=u(U);lastCookieString="";lastToken=null;parseCount=0;getToken(){let e=this.doc.cookie||"";return e!==this.lastCookieString&&(this.parseCount++,this.lastToken=ba(e,this.cookieName),this.lastCookieString=e),this.lastToken}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),EC=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=B(t1),r},providedIn:"root"})}return n})();function kC(n,t){if(!u(YR)||n.method==="GET"||n.method==="HEAD")return t(n);try{let r=u(jo).href,{origin:o}=new URL(r),{origin:s}=new URL(n.url,o);if(o!==s)return t(n)}catch(r){return t(n)}let e=u(EC).getToken(),i=u(e1);return e!=null&&!n.headers.has(i)&&(n=n.clone({headers:n.headers.set(i,e)})),t(n)}function SC(n,t){return t(n)}function n1(n,t){return(e,i)=>t.intercept(e,{handle:r=>n(r,i)})}function i1(n,t,e){return(i,r)=>wo(e,()=>t(i,o=>n(o,r)))}var IC=new v(""),cg=new v("",{factory:()=>[kC]}),MC=new v(""),dg=new v("",{factory:()=>!0});function r1(){let n=null;return(t,e)=>{n===null&&(n=(u(IC,{optional:!0})??[]).reduceRight(n1,SC));let i=u(Ks);if(u(dg)){let o=i.add();return n(t,e).pipe(fo(o))}else return n(t,e)}}var ug=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=B(lg),r},providedIn:"root"})}return n})();var Xd=(()=>{class n{backend;injector;chain=null;pendingTasks=u(Ks);contributeToStability=u(dg);constructor(e,i){this.backend=e,this.injector=i}handle(e){if(this.chain===null){let i=this.injector.get(Jd,null,{skipSelf:!0}),r=i!==null&&this.backend===i,o=this.injector.get(MC,[],r?{self:!0}:void 0),s=Array.from(new Set([...this.injector.get(cg),...o]));this.chain=s.reduceRight((a,l)=>i1(a,l,this.injector),SC)}if(this.contributeToStability){let i=this.pendingTasks.add();return this.chain(e,r=>this.backend.handle(r)).pipe(fo(i))}else return this.chain(e,i=>this.backend.handle(i))}static \u0275fac=function(i){return new(i||n)(B(ug),B(Ze))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Jd=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=B(Xd),r},providedIn:"root"})}return n})();function ag(n,t){return{body:t,headers:n.headers,context:n.context,observe:n.observe,params:n.params,reportProgress:n.reportProgress,responseType:n.responseType,withCredentials:n.withCredentials,credentials:n.credentials,transferCache:n.transferCache,timeout:n.timeout,keepalive:n.keepalive,priority:n.priority,cache:n.cache,mode:n.mode,redirect:n.redirect,integrity:n.integrity,referrer:n.referrer,referrerPolicy:n.referrerPolicy}}var ln=(()=>{class n{handler;constructor(e){this.handler=e}request(e,i,r={}){let o;if(e instanceof zo)o=e;else{let l;r.headers instanceof Hi?l=r.headers:l=new Hi(r.headers);let c;r.params&&(r.params instanceof ai?c=r.params:c=new ai({fromObject:r.params})),o=new zo(e,i,r.body!==void 0?r.body:null,{headers:l,context:r.context,params:c,reportProgress:r.reportProgress,responseType:r.responseType||"json",withCredentials:r.withCredentials,transferCache:r.transferCache,keepalive:r.keepalive,priority:r.priority,cache:r.cache,mode:r.mode,redirect:r.redirect,credentials:r.credentials,referrer:r.referrer,referrerPolicy:r.referrerPolicy,integrity:r.integrity,timeout:r.timeout})}let s=$e(o).pipe(Xm(l=>this.handler.handle(l)));if(e instanceof zo||r.observe==="events")return s;let a=s.pipe(ze(l=>l instanceof ka));switch(r.observe||"body"){case"body":switch(o.responseType){case"arraybuffer":return a.pipe(ye(l=>{if(l.body!==null&&!(l.body instanceof ArrayBuffer))throw new D(2806,!1);return l.body}));case"blob":return a.pipe(ye(l=>{if(l.body!==null&&!(l.body instanceof Blob))throw new D(2807,!1);return l.body}));case"text":return a.pipe(ye(l=>{if(l.body!==null&&typeof l.body!="string")throw new D(2808,!1);return l.body}));default:return a.pipe(ye(l=>l.body))}case"response":return a;default:throw new D(2809,!1)}}delete(e,i={}){return this.request("DELETE",e,i)}get(e,i={}){return this.request("GET",e,i)}head(e,i={}){return this.request("HEAD",e,i)}jsonp(e,i){return this.request("JSONP",e,{params:new ai().append(i,"JSONP_CALLBACK"),observe:"body",responseType:"json"})}options(e,i={}){return this.request("OPTIONS",e,i)}patch(e,i,r={}){return this.request("PATCH",e,ag(r,i))}post(e,i,r={}){return this.request("POST",e,ag(r,i))}put(e,i,r={}){return this.request("PUT",e,ag(r,i))}static \u0275fac=function(i){return new(i||n)(B(Jd))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var mg=(function(n){return n[n.Interceptors=0]="Interceptors",n[n.LegacyInterceptors=1]="LegacyInterceptors",n[n.CustomXsrfConfiguration=2]="CustomXsrfConfiguration",n[n.NoXsrfProtection=3]="NoXsrfProtection",n[n.JsonpSupport=4]="JsonpSupport",n[n.RequestsMadeViaParent=5]="RequestsMadeViaParent",n[n.Fetch=6]="Fetch",n})(mg||{});function o1(n,t){return{\u0275kind:n,\u0275providers:t}}function TC(...n){let t=[ln,Xd,{provide:Jd,useExisting:Xd},{provide:ug,useFactory:()=>u(QR,{optional:!0})??u(lg)},{provide:cg,useValue:kC,multi:!0}];for(let e of n)t.push(...e.\u0275providers);return Hs(t)}var CC=new v("");function AC(){return o1(mg.LegacyInterceptors,[{provide:CC,useFactory:r1},{provide:cg,useExisting:CC,multi:!0}])}var hg=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[TC(AC())]})}return n})();var Ia=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:function(i){let r=null;return i?r=new(i||n):r=B(s1),r},providedIn:"root"})}return n})(),s1=(()=>{class n extends Ia{_doc;constructor(e){super(),this._doc=e}sanitize(e,i){if(i==null)return null;switch(e){case ct.NONE:return i;case ct.HTML:return Pr(i,"HTML")?ri(i):ap(this._doc,String(i)).toString();case ct.STYLE:return Pr(i,"Style")?ri(i):i;case ct.SCRIPT:if(Pr(i,"Script"))return ri(i);throw new D(5200,!1);case ct.URL:return Pr(i,"URL")?ri(i):gd(String(i));case ct.RESOURCE_URL:if(Pr(i,"ResourceURL"))return ri(i);throw new D(5201,!1);default:throw new D(5202,!1)}}bypassSecurityTrustHtml(e){return np(e)}bypassSecurityTrustStyle(e){return ip(e)}bypassSecurityTrustScript(e){return rp(e)}bypassSecurityTrustUrl(e){return op(e)}bypassSecurityTrustResourceUrl(e){return sp(e)}static \u0275fac=function(i){return new(i||n)(B(U))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var me=(function(n){return n[n.State=0]="State",n[n.Transition=1]="Transition",n[n.Sequence=2]="Sequence",n[n.Group=3]="Group",n[n.Animate=4]="Animate",n[n.Keyframes=5]="Keyframes",n[n.Style=6]="Style",n[n.Trigger=7]="Trigger",n[n.Reference=8]="Reference",n[n.AnimateChild=9]="AnimateChild",n[n.AnimateRef=10]="AnimateRef",n[n.Query=11]="Query",n[n.Stagger=12]="Stagger",n})(me||{}),Cn="*";function OC(n,t=null){return{type:me.Sequence,steps:n,options:t}}function fg(n){return{type:me.Style,styles:n,offset:null}}var li=class{_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_originalOnDoneFns=[];_originalOnStartFns=[];_started=!1;_destroyed=!1;_finished=!1;_position=0;parentPlayer=null;totalTime;constructor(t=0,e=0){this.totalTime=t+e}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}onStart(t){this._originalOnStartFns.push(t),this._onStartFns.push(t)}onDone(t){this._originalOnDoneFns.push(t),this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}hasStarted(){return this._started}init(){}play(){this.hasStarted()||(this._onStart(),this.triggerMicrotask()),this._started=!0}triggerMicrotask(){queueMicrotask(()=>this._onFinish())}_onStart(){this._onStartFns.forEach(t=>t()),this._onStartFns=[]}pause(){}restart(){}finish(){this._onFinish()}destroy(){this._destroyed||(this._destroyed=!0,this.hasStarted()||this._onStart(),this.finish(),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}reset(){this._started=!1,this._finished=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}setPosition(t){this._position=this.totalTime?t*this.totalTime:1}getPosition(){return this.totalTime?this._position/this.totalTime:1}triggerCallback(t){let e=t=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},$o=class{_onDoneFns=[];_onStartFns=[];_finished=!1;_started=!1;_destroyed=!1;_onDestroyFns=[];parentPlayer=null;totalTime=0;players;constructor(t){this.players=t;let e=0,i=0,r=0,o=this.players.length;o==0?queueMicrotask(()=>this._onFinish()):this.players.forEach(s=>{s.onDone(()=>{++e==o&&this._onFinish()}),s.onDestroy(()=>{++i==o&&this._onDestroy()}),s.onStart(()=>{++r==o&&this._onStart()})}),this.totalTime=this.players.reduce((s,a)=>Math.max(s,a.totalTime),0)}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}init(){this.players.forEach(t=>t.init())}onStart(t){this._onStartFns.push(t)}_onStart(){this.hasStarted()||(this._started=!0,this._onStartFns.forEach(t=>t()),this._onStartFns=[])}onDone(t){this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}hasStarted(){return this._started}play(){this.parentPlayer||this.init(),this._onStart(),this.players.forEach(t=>t.play())}pause(){this.players.forEach(t=>t.pause())}restart(){this.players.forEach(t=>t.restart())}finish(){this._onFinish(),this.players.forEach(t=>t.finish())}destroy(){this._onDestroy()}_onDestroy(){this._destroyed||(this._destroyed=!0,this._onFinish(),this.players.forEach(t=>t.destroy()),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}reset(){this.players.forEach(t=>t.reset()),this._destroyed=!1,this._finished=!1,this._started=!1}setPosition(t){let e=t*this.totalTime;this.players.forEach(i=>{let r=i.totalTime?Math.min(1,e/i.totalTime):1;i.setPosition(r)})}getPosition(){let t=this.players.reduce((e,i)=>e===null||i.totalTime>e.totalTime?i:e,null);return t!=null?t.getPosition():0}beforeDestroy(){this.players.forEach(t=>{t.beforeDestroy&&t.beforeDestroy()})}triggerCallback(t){let e=t=="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},Ma="!";function NC(n){return new D(3e3,!1)}function a1(){return new D(3100,!1)}function l1(){return new D(3101,!1)}function c1(n){return new D(3001,!1)}function d1(n){return new D(3003,!1)}function u1(n){return new D(3004,!1)}function PC(n,t){return new D(3005,!1)}function LC(){return new D(3006,!1)}function BC(){return new D(3007,!1)}function VC(n,t){return new D(3008,!1)}function jC(n){return new D(3002,!1)}function zC(n,t,e,i,r){return new D(3010,!1)}function HC(){return new D(3011,!1)}function UC(){return new D(3012,!1)}function $C(){return new D(3200,!1)}function GC(){return new D(3202,!1)}function WC(){return new D(3013,!1)}function qC(n){return new D(3014,!1)}function QC(n){return new D(3015,!1)}function KC(n){return new D(3016,!1)}function YC(n,t){return new D(3404,!1)}function m1(n){return new D(3502,!1)}function ZC(n){return new D(3503,!1)}function XC(){return new D(3300,!1)}function JC(n){return new D(3504,!1)}function eD(n){return new D(3301,!1)}function tD(n,t){return new D(3302,!1)}function nD(n){return new D(3303,!1)}function iD(n,t){return new D(3400,!1)}function rD(n){return new D(3401,!1)}function oD(n){return new D(3402,!1)}function sD(n,t){return new D(3505,!1)}function ci(n){switch(n.length){case 0:return new li;case 1:return n[0];default:return new $o(n)}}function vg(n,t,e=new Map,i=new Map){let r=[],o=[],s=-1,a=null;if(t.forEach(l=>{let c=l.get("offset"),d=c==s,m=d&&a||new Map;l.forEach((f,h)=>{let p=h,b=f;if(h!=="offset")switch(p=n.normalizePropertyName(p,r),b){case Ma:b=e.get(h);break;case Cn:b=i.get(h);break;default:b=n.normalizeStyleValue(h,p,b,r);break}m.set(p,b)}),d||o.push(m),a=m,s=c}),r.length)throw m1(r);return o}function eu(n,t,e,i){switch(t){case"start":n.onStart(()=>i(e&&pg(e,"start",n)));break;case"done":n.onDone(()=>i(e&&pg(e,"done",n)));break;case"destroy":n.onDestroy(()=>i(e&&pg(e,"destroy",n)));break}}function pg(n,t,e){let i=e.totalTime,r=!!e.disabled,o=tu(n.element,n.triggerName,n.fromState,n.toState,t||n.phaseName,i??n.totalTime,r),s=n._data;return s!=null&&(o._data=s),o}function tu(n,t,e,i,r="",o=0,s){return{element:n,triggerName:t,fromState:e,toState:i,phaseName:r,totalTime:o,disabled:!!s}}function Ft(n,t,e){let i=n.get(t);return i||n.set(t,i=e),i}function bg(n){let t=n.indexOf(":"),e=n.substring(1,t),i=n.slice(t+1);return[e,i]}var h1=typeof document>"u"?null:document.documentElement;function nu(n){let t=n.parentNode||n.host||null;return t===h1?null:t}function f1(n){return n.substring(1,6)=="ebkit"}var Vr=null,FC=!1;function aD(n){Vr||(Vr=p1()||{},FC=Vr.style?"WebkitAppearance"in Vr.style:!1);let t=!0;return Vr.style&&!f1(n)&&(t=n in Vr.style,!t&&FC&&(t="Webkit"+n.charAt(0).toUpperCase()+n.slice(1)in Vr.style)),t}function p1(){return typeof document<"u"?document.body:null}function yg(n,t){for(;t;){if(t===n)return!0;t=nu(t)}return!1}function wg(n,t,e){if(e)return Array.from(n.querySelectorAll(t));let i=n.querySelector(t);return i?[i]:[]}var g1=1e3,Cg="{{",_1="}}",Dg="ng-enter",iu="ng-leave",Ta="ng-trigger",Aa=".ng-trigger",xg="ng-animating",ru=".ng-animating";function $n(n){if(typeof n=="number")return n;let t=n.match(/^(-?[\.\d]+)(m?s)/);return!t||t.length<2?0:gg(parseFloat(t[1]),t[2])}function gg(n,t){return t==="s"?n*g1:n}function Ra(n,t,e){return n.hasOwnProperty("duration")?n:b1(n,t,e)}var v1=/^(-?[\.\d]+)(m?s)(?:\s+(-?[\.\d]+)(m?s))?(?:\s+([-a-z]+(?:\(.+?\))?))?$/i;function b1(n,t,e){let i,r=0,o="";if(typeof n=="string"){let s=n.match(v1);if(s===null)return t.push(NC(n)),{duration:0,delay:0,easing:""};i=gg(parseFloat(s[1]),s[2]);let a=s[3];a!=null&&(r=gg(parseFloat(a),s[4]));let l=s[5];l&&(o=l)}else i=n;if(!e){let s=!1,a=t.length;i<0&&(t.push(a1()),s=!0),r<0&&(t.push(l1()),s=!0),s&&t.splice(a,0,NC(n))}return{duration:i,delay:r,easing:o}}function lD(n){return n.length?n[0]instanceof Map?n:n.map(t=>new Map(Object.entries(t))):[]}function Dn(n,t,e){t.forEach((i,r)=>{let o=ou(r);e&&!e.has(r)&&e.set(r,n.style[o]),n.style[o]=i})}function Ui(n,t){t.forEach((e,i)=>{let r=ou(i);n.style[r]=""})}function Go(n){return Array.isArray(n)?n.length==1?n[0]:OC(n):n}function cD(n,t,e){let i=t.params||{},r=Eg(n);r.length&&r.forEach(o=>{i.hasOwnProperty(o)||e.push(c1(o))})}var _g=new RegExp(`${Cg}\\s*(.+?)\\s*${_1}`,"g");function Eg(n){let t=[];if(typeof n=="string"){let e;for(;e=_g.exec(n);)t.push(e[1]);_g.lastIndex=0}return t}function Wo(n,t,e){let i=`${n}`,r=i.replace(_g,(o,s)=>{let a=t[s];return a==null&&(e.push(d1(s)),a=""),a.toString()});return r==i?n:r}var y1=/-+([a-z0-9])/g;function ou(n){return n.replace(y1,(...t)=>t[1].toUpperCase())}function dD(n,t){return n===0||t===0}function uD(n,t,e){if(e.size&&t.length){let i=t[0],r=[];if(e.forEach((o,s)=>{i.has(s)||r.push(s),i.set(s,o)}),r.length)for(let o=1;o<t.length;o++){let s=t[o];r.forEach(a=>s.set(a,su(n,a)))}}return t}function Pt(n,t,e){switch(t.type){case me.Trigger:return n.visitTrigger(t,e);case me.State:return n.visitState(t,e);case me.Transition:return n.visitTransition(t,e);case me.Sequence:return n.visitSequence(t,e);case me.Group:return n.visitGroup(t,e);case me.Animate:return n.visitAnimate(t,e);case me.Keyframes:return n.visitKeyframes(t,e);case me.Style:return n.visitStyle(t,e);case me.Reference:return n.visitReference(t,e);case me.AnimateChild:return n.visitAnimateChild(t,e);case me.AnimateRef:return n.visitAnimateRef(t,e);case me.Query:return n.visitQuery(t,e);case me.Stagger:return n.visitStagger(t,e);default:throw u1(t.type)}}function su(n,t){return window.getComputedStyle(n)[t]}var Hg=(()=>{class n{validateStyleProperty(e){return aD(e)}containsElement(e,i){return yg(e,i)}getParentElement(e){return nu(e)}query(e,i,r){return wg(e,i,r)}computeStyle(e,i,r){return r||""}animate(e,i,r,o,s,a=[],l){return new li(r,o)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})(),zr=class{static NOOP=new Hg},Hr=class{};var w1=new Set(["width","height","minWidth","minHeight","maxWidth","maxHeight","left","top","bottom","right","fontSize","outlineWidth","outlineOffset","paddingTop","paddingLeft","paddingBottom","paddingRight","marginTop","marginLeft","marginBottom","marginRight","borderRadius","borderWidth","borderTopWidth","borderLeftWidth","borderRightWidth","borderBottomWidth","textIndent","perspective"]),uu=class extends Hr{normalizePropertyName(t,e){return ou(t)}normalizeStyleValue(t,e,i,r){let o="",s=i.toString().trim();if(w1.has(e)&&i!==0&&i!=="0")if(typeof i=="number")o="px";else{let a=i.match(/^[+-]?[\d\.]+([a-z]*)$/);a&&a[1].length==0&&r.push(PC(t,i))}return s+o}};var mu="*";function C1(n,t){let e=[];return typeof n=="string"?n.split(/\s*,\s*/).forEach(i=>D1(i,e,t)):e.push(n),e}function D1(n,t,e){if(n[0]==":"){let l=x1(n,e);if(typeof l=="function"){t.push(l);return}n=l}let i=n.match(/^(\*|[-\w]+)\s*(<?[=-]>)\s*(\*|[-\w]+)$/);if(i==null||i.length<4)return e.push(QC(n)),t;let r=i[1],o=i[2],s=i[3];t.push(mD(r,s));let a=r==mu&&s==mu;o[0]=="<"&&!a&&t.push(mD(s,r))}function x1(n,t){switch(n){case":enter":return"void => *";case":leave":return"* => void";case":increment":return(e,i)=>parseFloat(i)>parseFloat(e);case":decrement":return(e,i)=>parseFloat(i)<parseFloat(e);default:return t.push(KC(n)),"* => *"}}var au=new Set(["true","1"]),lu=new Set(["false","0"]);function mD(n,t){let e=au.has(n)||lu.has(n),i=au.has(t)||lu.has(t);return(r,o)=>{let s=n==mu||n==r,a=t==mu||t==o;return!s&&e&&typeof r=="boolean"&&(s=r?au.has(n):lu.has(n)),!a&&i&&typeof o=="boolean"&&(a=o?au.has(t):lu.has(t)),s&&a}}var CD=":self",E1=new RegExp(`s*${CD}s*,?`,"g");function DD(n,t,e,i){return new Ag(n).build(t,e,i)}var hD="",Ag=class{_driver;constructor(t){this._driver=t}build(t,e,i){let r=new Rg(e);return this._resetContextStyleTimingState(r),Pt(this,Go(t),r)}_resetContextStyleTimingState(t){t.currentQuerySelector=hD,t.collectedStyles=new Map,t.collectedStyles.set(hD,new Map),t.currentTime=0}visitTrigger(t,e){let i=e.queryCount=0,r=e.depCount=0,o=[],s=[];return t.name.charAt(0)=="@"&&e.errors.push(LC()),t.definitions.forEach(a=>{if(this._resetContextStyleTimingState(e),a.type==me.State){let l=a,c=l.name;c.toString().split(/\s*,\s*/).forEach(d=>{l.name=d,o.push(this.visitState(l,e))}),l.name=c}else if(a.type==me.Transition){let l=this.visitTransition(a,e);i+=l.queryCount,r+=l.depCount,s.push(l)}else e.errors.push(BC())}),{type:me.Trigger,name:t.name,states:o,transitions:s,queryCount:i,depCount:r,options:null}}visitState(t,e){let i=this.visitStyle(t.styles,e),r=t.options&&t.options.params||null;if(i.containsDynamicStyles){let o=new Set,s=r||{};i.styles.forEach(a=>{a instanceof Map&&a.forEach(l=>{Eg(l).forEach(c=>{s.hasOwnProperty(c)||o.add(c)})})}),o.size&&e.errors.push(VC(t.name,[...o.values()]))}return{type:me.State,name:t.name,style:i,options:r?{params:r}:null}}visitTransition(t,e){e.queryCount=0,e.depCount=0;let i=Pt(this,Go(t.animation),e),r=C1(t.expr,e.errors);return{type:me.Transition,matchers:r,animation:i,queryCount:e.queryCount,depCount:e.depCount,options:jr(t.options)}}visitSequence(t,e){return{type:me.Sequence,steps:t.steps.map(i=>Pt(this,i,e)),options:jr(t.options)}}visitGroup(t,e){let i=e.currentTime,r=0,o=t.steps.map(s=>{e.currentTime=i;let a=Pt(this,s,e);return r=Math.max(r,e.currentTime),a});return e.currentTime=r,{type:me.Group,steps:o,options:jr(t.options)}}visitAnimate(t,e){let i=M1(t.timings,e.errors);e.currentAnimateTimings=i;let r,o=t.styles?t.styles:fg({});if(o.type==me.Keyframes)r=this.visitKeyframes(o,e);else{let s=t.styles,a=!1;if(!s){a=!0;let c={};i.easing&&(c.easing=i.easing),s=fg(c)}e.currentTime+=i.duration+i.delay;let l=this.visitStyle(s,e);l.isEmptyStep=a,r=l}return e.currentAnimateTimings=null,{type:me.Animate,timings:i,style:r,options:null}}visitStyle(t,e){let i=this._makeStyleAst(t,e);return this._validateStyleAst(i,e),i}_makeStyleAst(t,e){let i=[],r=Array.isArray(t.styles)?t.styles:[t.styles];for(let a of r)typeof a=="string"?a===Cn?i.push(a):e.errors.push(jC(a)):i.push(new Map(Object.entries(a)));let o=!1,s=null;return i.forEach(a=>{if(a instanceof Map&&(a.has("easing")&&(s=a.get("easing"),a.delete("easing")),!o)){for(let l of a.values())if(l.toString().indexOf(Cg)>=0){o=!0;break}}}),{type:me.Style,styles:i,easing:s,offset:t.offset,containsDynamicStyles:o,options:null}}_validateStyleAst(t,e){let i=e.currentAnimateTimings,r=e.currentTime,o=e.currentTime;i&&o>0&&(o-=i.duration+i.delay),t.styles.forEach(s=>{typeof s!="string"&&s.forEach((a,l)=>{let c=e.collectedStyles.get(e.currentQuerySelector),d=c.get(l),m=!0;d&&(o!=r&&o>=d.startTime&&r<=d.endTime&&(e.errors.push(zC(l,d.startTime,d.endTime,o,r)),m=!1),o=d.startTime),m&&c.set(l,{startTime:o,endTime:r}),e.options&&cD(a,e.options,e.errors)})})}visitKeyframes(t,e){let i={type:me.Keyframes,styles:[],options:null};if(!e.currentAnimateTimings)return e.errors.push(HC()),i;let r=1,o=0,s=[],a=!1,l=!1,c=0,d=t.steps.map(x=>{let ee=this._makeStyleAst(x,e),ke=ee.offset!=null?ee.offset:I1(ee.styles),pe=0;return ke!=null&&(o++,pe=ee.offset=ke),l=l||pe<0||pe>1,a=a||pe<c,c=pe,s.push(pe),ee});l&&e.errors.push(UC()),a&&e.errors.push($C());let m=t.steps.length,f=0;o>0&&o<m?e.errors.push(GC()):o==0&&(f=r/(m-1));let h=m-1,p=e.currentTime,b=e.currentAnimateTimings,C=b.duration;return d.forEach((x,ee)=>{let ke=f>0?ee==h?1:f*ee:s[ee],pe=ke*C;e.currentTime=p+b.delay+pe,b.duration=pe,this._validateStyleAst(x,e),x.offset=ke,i.styles.push(x)}),i}visitReference(t,e){return{type:me.Reference,animation:Pt(this,Go(t.animation),e),options:jr(t.options)}}visitAnimateChild(t,e){return e.depCount++,{type:me.AnimateChild,options:jr(t.options)}}visitAnimateRef(t,e){return{type:me.AnimateRef,animation:this.visitReference(t.animation,e),options:jr(t.options)}}visitQuery(t,e){let i=e.currentQuerySelector,r=t.options||{};e.queryCount++,e.currentQuery=t;let[o,s]=k1(t.selector);e.currentQuerySelector=i.length?i+" "+o:o,Ft(e.collectedStyles,e.currentQuerySelector,new Map);let a=Pt(this,Go(t.animation),e);return e.currentQuery=null,e.currentQuerySelector=i,{type:me.Query,selector:o,limit:r.limit||0,optional:!!r.optional,includeSelf:s,animation:a,originalSelector:t.selector,options:jr(t.options)}}visitStagger(t,e){e.currentQuery||e.errors.push(WC());let i=t.timings==="full"?{duration:0,delay:0,easing:"full"}:Ra(t.timings,e.errors,!0);return{type:me.Stagger,animation:Pt(this,Go(t.animation),e),timings:i,options:null}}};function k1(n){let t=!!n.split(/\s*,\s*/).find(e=>e==CD);return t&&(n=n.replace(E1,"")),n=n.replace(/@\*/g,Aa).replace(/@\w+/g,e=>Aa+"-"+e.slice(1)).replace(/:animating/g,ru),[n,t]}function S1(n){return n?M({},n):null}var Rg=class{errors;queryCount=0;depCount=0;currentTransition=null;currentQuery=null;currentQuerySelector=null;currentAnimateTimings=null;currentTime=0;collectedStyles=new Map;options=null;unsupportedCSSPropertiesFound=new Set;constructor(t){this.errors=t}};function I1(n){if(typeof n=="string")return null;let t=null;if(Array.isArray(n))n.forEach(e=>{if(e instanceof Map&&e.has("offset")){let i=e;t=parseFloat(i.get("offset")),i.delete("offset")}});else if(n instanceof Map&&n.has("offset")){let e=n;t=parseFloat(e.get("offset")),e.delete("offset")}return t}function M1(n,t){if(n.hasOwnProperty("duration"))return n;if(typeof n=="number"){let o=Ra(n,t).duration;return kg(o,0,"")}let e=n;if(e.split(/\s+/).some(o=>o.charAt(0)=="{"&&o.charAt(1)=="{")){let o=kg(0,0,"");return o.dynamic=!0,o.strValue=e,o}let r=Ra(e,t);return kg(r.duration,r.delay,r.easing)}function jr(n){return n?(n=M({},n),n.params&&(n.params=S1(n.params))):n={},n}function kg(n,t,e){return{duration:n,delay:t,easing:e}}function Ug(n,t,e,i,r,o,s=null,a=!1){return{type:1,element:n,keyframes:t,preStyleProps:e,postStyleProps:i,duration:r,delay:o,totalTime:r+o,easing:s,subTimeline:a}}var Na=class{_map=new Map;get(t){return this._map.get(t)||[]}append(t,e){let i=this._map.get(t);i||this._map.set(t,i=[]),i.push(...e)}has(t){return this._map.has(t)}clear(){this._map.clear()}},T1=1,A1=":enter",R1=new RegExp(A1,"g"),O1=":leave",N1=new RegExp(O1,"g");function xD(n,t,e,i,r,o=new Map,s=new Map,a,l,c=[]){return new Og().buildKeyframes(n,t,e,i,r,o,s,a,l,c)}var Og=class{buildKeyframes(t,e,i,r,o,s,a,l,c,d=[]){c=c||new Na;let m=new Ng(t,e,c,r,o,d,[]);m.options=l;let f=l.delay?$n(l.delay):0;m.currentTimeline.delayNextStep(f),m.currentTimeline.setStyles([s],null,m.errors,l),Pt(this,i,m);let h=m.timelines.filter(p=>p.containsAnimation());if(h.length&&a.size){let p;for(let b=h.length-1;b>=0;b--){let C=h[b];if(C.element===e){p=C;break}}p&&!p.allowOnlyTimelineStyles()&&p.setStyles([a],null,m.errors,l)}return h.length?h.map(p=>p.buildKeyframes()):[Ug(e,[],[],[],0,f,"",!1)]}visitTrigger(t,e){}visitState(t,e){}visitTransition(t,e){}visitAnimateChild(t,e){let i=e.subInstructions.get(e.element);if(i){let r=e.createSubContext(t.options),o=e.currentTimeline.currentTime,s=this._visitSubInstructions(i,r,r.options);o!=s&&e.transformIntoNewTimeline(s)}e.previousNode=t}visitAnimateRef(t,e){let i=e.createSubContext(t.options);i.transformIntoNewTimeline(),this._applyAnimationRefDelays([t.options,t.animation.options],e,i),this.visitReference(t.animation,i),e.transformIntoNewTimeline(i.currentTimeline.currentTime),e.previousNode=t}_applyAnimationRefDelays(t,e,i){for(let r of t){let o=r?.delay;if(o){let s=typeof o=="number"?o:$n(Wo(o,r?.params??{},e.errors));i.delayNextStep(s)}}}_visitSubInstructions(t,e,i){let o=e.currentTimeline.currentTime,s=i.duration!=null?$n(i.duration):null,a=i.delay!=null?$n(i.delay):null;return s!==0&&t.forEach(l=>{let c=e.appendInstructionToTimeline(l,s,a);o=Math.max(o,c.duration+c.delay)}),o}visitReference(t,e){e.updateOptions(t.options,!0),Pt(this,t.animation,e),e.previousNode=t}visitSequence(t,e){let i=e.subContextCount,r=e,o=t.options;if(o&&(o.params||o.delay)&&(r=e.createSubContext(o),r.transformIntoNewTimeline(),o.delay!=null)){r.previousNode.type==me.Style&&(r.currentTimeline.snapshotCurrentStyles(),r.previousNode=hu);let s=$n(o.delay);r.delayNextStep(s)}t.steps.length&&(t.steps.forEach(s=>Pt(this,s,r)),r.currentTimeline.applyStylesToKeyframe(),r.subContextCount>i&&r.transformIntoNewTimeline()),e.previousNode=t}visitGroup(t,e){let i=[],r=e.currentTimeline.currentTime,o=t.options&&t.options.delay?$n(t.options.delay):0;t.steps.forEach(s=>{let a=e.createSubContext(t.options);o&&a.delayNextStep(o),Pt(this,s,a),r=Math.max(r,a.currentTimeline.currentTime),i.push(a.currentTimeline)}),i.forEach(s=>e.currentTimeline.mergeTimelineCollectedStyles(s)),e.transformIntoNewTimeline(r),e.previousNode=t}_visitTiming(t,e){if(t.dynamic){let i=t.strValue,r=e.params?Wo(i,e.params,e.errors):i;return Ra(r,e.errors)}else return{duration:t.duration,delay:t.delay,easing:t.easing}}visitAnimate(t,e){let i=e.currentAnimateTimings=this._visitTiming(t.timings,e),r=e.currentTimeline;i.delay&&(e.incrementTime(i.delay),r.snapshotCurrentStyles());let o=t.style;o.type==me.Keyframes?this.visitKeyframes(o,e):(e.incrementTime(i.duration),this.visitStyle(o,e),r.applyStylesToKeyframe()),e.currentAnimateTimings=null,e.previousNode=t}visitStyle(t,e){let i=e.currentTimeline,r=e.currentAnimateTimings;!r&&i.hasCurrentStyleProperties()&&i.forwardFrame();let o=r&&r.easing||t.easing;t.isEmptyStep?i.applyEmptyStep(o):i.setStyles(t.styles,o,e.errors,e.options),e.previousNode=t}visitKeyframes(t,e){let i=e.currentAnimateTimings,r=e.currentTimeline.duration,o=i.duration,a=e.createSubContext().currentTimeline;a.easing=i.easing,t.styles.forEach(l=>{let c=l.offset||0;a.forwardTime(c*o),a.setStyles(l.styles,l.easing,e.errors,e.options),a.applyStylesToKeyframe()}),e.currentTimeline.mergeTimelineCollectedStyles(a),e.transformIntoNewTimeline(r+o),e.previousNode=t}visitQuery(t,e){let i=e.currentTimeline.currentTime,r=t.options||{},o=r.delay?$n(r.delay):0;o&&(e.previousNode.type===me.Style||i==0&&e.currentTimeline.hasCurrentStyleProperties())&&(e.currentTimeline.snapshotCurrentStyles(),e.previousNode=hu);let s=i,a=e.invokeQuery(t.selector,t.originalSelector,t.limit,t.includeSelf,!!r.optional,e.errors);e.currentQueryTotal=a.length;let l=null;a.forEach((c,d)=>{e.currentQueryIndex=d;let m=e.createSubContext(t.options,c);o&&m.delayNextStep(o),c===e.element&&(l=m.currentTimeline),Pt(this,t.animation,m),m.currentTimeline.applyStylesToKeyframe();let f=m.currentTimeline.currentTime;s=Math.max(s,f)}),e.currentQueryIndex=0,e.currentQueryTotal=0,e.transformIntoNewTimeline(s),l&&(e.currentTimeline.mergeTimelineCollectedStyles(l),e.currentTimeline.snapshotCurrentStyles()),e.previousNode=t}visitStagger(t,e){let i=e.parentContext,r=e.currentTimeline,o=t.timings,s=Math.abs(o.duration),a=s*(e.currentQueryTotal-1),l=s*e.currentQueryIndex;switch(o.duration<0?"reverse":o.easing){case"reverse":l=a-l;break;case"full":l=i.currentStaggerTime;break}let d=e.currentTimeline;l&&d.delayNextStep(l);let m=d.currentTime;Pt(this,t.animation,e),e.previousNode=t,i.currentStaggerTime=r.currentTime-m+(r.startTime-i.currentTimeline.startTime)}},hu={},Ng=class n{_driver;element;subInstructions;_enterClassName;_leaveClassName;errors;timelines;parentContext=null;currentTimeline;currentAnimateTimings=null;previousNode=hu;subContextCount=0;options={};currentQueryIndex=0;currentQueryTotal=0;currentStaggerTime=0;constructor(t,e,i,r,o,s,a,l){this._driver=t,this.element=e,this.subInstructions=i,this._enterClassName=r,this._leaveClassName=o,this.errors=s,this.timelines=a,this.currentTimeline=l||new fu(this._driver,e,0),a.push(this.currentTimeline)}get params(){return this.options.params}updateOptions(t,e){if(!t)return;let i=t,r=this.options;i.duration!=null&&(r.duration=$n(i.duration)),i.delay!=null&&(r.delay=$n(i.delay));let o=i.params;if(o){let s=r.params;s||(s=this.options.params={}),Object.keys(o).forEach(a=>{(!e||!s.hasOwnProperty(a))&&(s[a]=Wo(o[a],s,this.errors))})}}_copyOptions(){let t={};if(this.options){let e=this.options.params;if(e){let i=t.params={};Object.keys(e).forEach(r=>{i[r]=e[r]})}}return t}createSubContext(t=null,e,i){let r=e||this.element,o=new n(this._driver,r,this.subInstructions,this._enterClassName,this._leaveClassName,this.errors,this.timelines,this.currentTimeline.fork(r,i||0));return o.previousNode=this.previousNode,o.currentAnimateTimings=this.currentAnimateTimings,o.options=this._copyOptions(),o.updateOptions(t),o.currentQueryIndex=this.currentQueryIndex,o.currentQueryTotal=this.currentQueryTotal,o.parentContext=this,this.subContextCount++,o}transformIntoNewTimeline(t){return this.previousNode=hu,this.currentTimeline=this.currentTimeline.fork(this.element,t),this.timelines.push(this.currentTimeline),this.currentTimeline}appendInstructionToTimeline(t,e,i){let r={duration:e??t.duration,delay:this.currentTimeline.currentTime+(i??0)+t.delay,easing:""},o=new Fg(this._driver,t.element,t.keyframes,t.preStyleProps,t.postStyleProps,r,t.stretchStartingKeyframe);return this.timelines.push(o),r}incrementTime(t){this.currentTimeline.forwardTime(this.currentTimeline.duration+t)}delayNextStep(t){t>0&&this.currentTimeline.delayNextStep(t)}invokeQuery(t,e,i,r,o,s){let a=[];if(r&&a.push(this.element),t.length>0){t=t.replace(R1,"."+this._enterClassName),t=t.replace(N1,"."+this._leaveClassName);let l=i!=1,c=this._driver.query(this.element,t,l);i!==0&&(c=i<0?c.slice(c.length+i,c.length):c.slice(0,i)),a.push(...c)}return!o&&a.length==0&&s.push(qC(e)),a}},fu=class n{_driver;element;startTime;_elementTimelineStylesLookup;duration=0;easing=null;_previousKeyframe=new Map;_currentKeyframe=new Map;_keyframes=new Map;_styleSummary=new Map;_localTimelineStyles=new Map;_globalTimelineStyles;_pendingStyles=new Map;_backFill=new Map;_currentEmptyStepKeyframe=null;constructor(t,e,i,r){this._driver=t,this.element=e,this.startTime=i,this._elementTimelineStylesLookup=r,this._elementTimelineStylesLookup||(this._elementTimelineStylesLookup=new Map),this._globalTimelineStyles=this._elementTimelineStylesLookup.get(e),this._globalTimelineStyles||(this._globalTimelineStyles=this._localTimelineStyles,this._elementTimelineStylesLookup.set(e,this._localTimelineStyles)),this._loadKeyframe()}containsAnimation(){switch(this._keyframes.size){case 0:return!1;case 1:return this.hasCurrentStyleProperties();default:return!0}}hasCurrentStyleProperties(){return this._currentKeyframe.size>0}get currentTime(){return this.startTime+this.duration}delayNextStep(t){let e=this._keyframes.size===1&&this._pendingStyles.size;this.duration||e?(this.forwardTime(this.currentTime+t),e&&this.snapshotCurrentStyles()):this.startTime+=t}fork(t,e){return this.applyStylesToKeyframe(),new n(this._driver,t,e||this.currentTime,this._elementTimelineStylesLookup)}_loadKeyframe(){this._currentKeyframe&&(this._previousKeyframe=this._currentKeyframe),this._currentKeyframe=this._keyframes.get(this.duration),this._currentKeyframe||(this._currentKeyframe=new Map,this._keyframes.set(this.duration,this._currentKeyframe))}forwardFrame(){this.duration+=T1,this._loadKeyframe()}forwardTime(t){this.applyStylesToKeyframe(),this.duration=t,this._loadKeyframe()}_updateStyle(t,e){this._localTimelineStyles.set(t,e),this._globalTimelineStyles.set(t,e),this._styleSummary.set(t,{time:this.currentTime,value:e})}allowOnlyTimelineStyles(){return this._currentEmptyStepKeyframe!==this._currentKeyframe}applyEmptyStep(t){t&&this._previousKeyframe.set("easing",t);for(let[e,i]of this._globalTimelineStyles)this._backFill.set(e,i||Cn),this._currentKeyframe.set(e,Cn);this._currentEmptyStepKeyframe=this._currentKeyframe}setStyles(t,e,i,r){e&&this._previousKeyframe.set("easing",e);let o=r&&r.params||{},s=F1(t,this._globalTimelineStyles);for(let[a,l]of s){let c=Wo(l,o,i);this._pendingStyles.set(a,c),this._localTimelineStyles.has(a)||this._backFill.set(a,this._globalTimelineStyles.get(a)??Cn),this._updateStyle(a,c)}}applyStylesToKeyframe(){this._pendingStyles.size!=0&&(this._pendingStyles.forEach((t,e)=>{this._currentKeyframe.set(e,t)}),this._pendingStyles.clear(),this._localTimelineStyles.forEach((t,e)=>{this._currentKeyframe.has(e)||this._currentKeyframe.set(e,t)}))}snapshotCurrentStyles(){for(let[t,e]of this._localTimelineStyles)this._pendingStyles.set(t,e),this._updateStyle(t,e)}getFinalKeyframe(){return this._keyframes.get(this.duration)}get properties(){let t=[];for(let e in this._currentKeyframe)t.push(e);return t}mergeTimelineCollectedStyles(t){t._styleSummary.forEach((e,i)=>{let r=this._styleSummary.get(i);(!r||e.time>r.time)&&this._updateStyle(i,e.value)})}buildKeyframes(){this.applyStylesToKeyframe();let t=new Set,e=new Set,i=this._keyframes.size===1&&this.duration===0,r=[];this._keyframes.forEach((a,l)=>{let c=new Map([...this._backFill,...a]);c.forEach((d,m)=>{d===Ma?t.add(m):d===Cn&&e.add(m)}),i||c.set("offset",l/this.duration),r.push(c)});let o=[...t.values()],s=[...e.values()];if(i){let a=r[0],l=new Map(a);a.set("offset",0),l.set("offset",1),r=[a,l]}return Ug(this.element,r,o,s,this.duration,this.startTime,this.easing,!1)}},Fg=class extends fu{keyframes;preStyleProps;postStyleProps;_stretchStartingKeyframe;timings;constructor(t,e,i,r,o,s,a=!1){super(t,e,s.delay),this.keyframes=i,this.preStyleProps=r,this.postStyleProps=o,this._stretchStartingKeyframe=a,this.timings={duration:s.duration,delay:s.delay,easing:s.easing}}containsAnimation(){return this.keyframes.length>1}buildKeyframes(){let t=this.keyframes,{delay:e,duration:i,easing:r}=this.timings;if(this._stretchStartingKeyframe&&e){let o=[],s=i+e,a=e/s,l=new Map(t[0]);l.set("offset",0),o.push(l);let c=new Map(t[0]);c.set("offset",fD(a)),o.push(c);let d=t.length-1;for(let m=1;m<=d;m++){let f=new Map(t[m]),h=f.get("offset"),p=e+h*i;f.set("offset",fD(p/s)),o.push(f)}i=s,e=0,r="",t=o}return Ug(this.element,t,this.preStyleProps,this.postStyleProps,i,e,r,!0)}};function fD(n,t=3){let e=Math.pow(10,t-1);return Math.round(n*e)/e}function F1(n,t){let e=new Map,i;return n.forEach(r=>{if(r==="*"){i??=t.keys();for(let o of i)e.set(o,Cn)}else for(let[o,s]of r)e.set(o,s)}),e}function pD(n,t,e,i,r,o,s,a,l,c,d,m,f){return{type:0,element:n,triggerName:t,isRemovalTransition:r,fromState:e,fromStyles:o,toState:i,toStyles:s,timelines:a,queriedElements:l,preStyleProps:c,postStyleProps:d,totalTime:m,errors:f}}var Sg={},pu=class{_triggerName;ast;_stateStyles;constructor(t,e,i){this._triggerName=t,this.ast=e,this._stateStyles=i}match(t,e,i,r){return P1(this.ast.matchers,t,e,i,r)}buildStyles(t,e,i){let r=this._stateStyles.get("*");return t!==void 0&&(r=this._stateStyles.get(t?.toString())||r),r?r.buildStyles(e,i):new Map}build(t,e,i,r,o,s,a,l,c,d){let m=[],f=this.ast.options&&this.ast.options.params||Sg,h=a&&a.params||Sg,p=this.buildStyles(i,h,m),b=l&&l.params||Sg,C=this.buildStyles(r,b,m),x=new Set,ee=new Map,ke=new Map,pe=r==="void",qt={params:ED(b,f),delay:this.ast.options?.delay},pt=d?[]:xD(t,e,this.ast.animation,o,s,p,C,qt,c,m),Ve=0;return pt.forEach(Mt=>{Ve=Math.max(Mt.duration+Mt.delay,Ve)}),m.length?pD(e,this._triggerName,i,r,pe,p,C,[],[],ee,ke,Ve,m):(pt.forEach(Mt=>{let ir=Mt.element,to=Ft(ee,ir,new Set);Mt.preStyleProps.forEach(rr=>to.add(rr));let mv=Ft(ke,ir,new Set);Mt.postStyleProps.forEach(rr=>mv.add(rr)),ir!==e&&x.add(ir)}),pD(e,this._triggerName,i,r,pe,p,C,pt,[...x.values()],ee,ke,Ve))}};function P1(n,t,e,i,r){return n.some(o=>o(t,e,i,r))}function ED(n,t){let e=M({},t);return Object.entries(n).forEach(([i,r])=>{r!=null&&(e[i]=r)}),e}var Pg=class{styles;defaultParams;normalizer;constructor(t,e,i){this.styles=t,this.defaultParams=e,this.normalizer=i}buildStyles(t,e){let i=new Map,r=ED(t,this.defaultParams);return this.styles.styles.forEach(o=>{typeof o!="string"&&o.forEach((s,a)=>{s&&(s=Wo(s,r,e));let l=this.normalizer.normalizePropertyName(a,e);s=this.normalizer.normalizeStyleValue(a,l,s,e),i.set(a,s)})}),i}};function L1(n,t,e){return new Lg(n,t,e)}var Lg=class{name;ast;_normalizer;transitionFactories=[];fallbackTransition;states=new Map;constructor(t,e,i){this.name=t,this.ast=e,this._normalizer=i,e.states.forEach(r=>{let o=r.options&&r.options.params||{};this.states.set(r.name,new Pg(r.style,o,i))}),gD(this.states,"true","1"),gD(this.states,"false","0"),e.transitions.forEach(r=>{this.transitionFactories.push(new pu(t,r,this.states))}),this.fallbackTransition=B1(t,this.states)}get containsQueries(){return this.ast.queryCount>0}matchTransition(t,e,i,r){return this.transitionFactories.find(s=>s.match(t,e,i,r))||null}matchStyles(t,e,i){return this.fallbackTransition.buildStyles(t,e,i)}};function B1(n,t,e){let i=[(s,a)=>!0],r={type:me.Sequence,steps:[],options:null},o={type:me.Transition,animation:r,matchers:i,options:null,queryCount:0,depCount:0};return new pu(n,o,t)}function gD(n,t,e){n.has(t)?n.has(e)||n.set(e,n.get(t)):n.has(e)&&n.set(t,n.get(e))}var V1=new Na,Bg=class{bodyNode;_driver;_normalizer;_animations=new Map;_playersById=new Map;players=[];constructor(t,e,i){this.bodyNode=t,this._driver=e,this._normalizer=i}register(t,e){let i=[],r=[],o=DD(this._driver,e,i,r);if(i.length)throw ZC(i);this._animations.set(t,o)}_buildPlayer(t,e,i){let r=t.element,o=vg(this._normalizer,t.keyframes,e,i);return this._driver.animate(r,o,t.duration,t.delay,t.easing,[],!0)}create(t,e,i={}){let r=[],o=this._animations.get(t),s,a=new Map;if(o?(s=xD(this._driver,e,o,Dg,iu,new Map,new Map,i,V1,r),s.forEach(d=>{let m=Ft(a,d.element,new Map);d.postStyleProps.forEach(f=>m.set(f,null))})):(r.push(XC()),s=[]),r.length)throw JC(r);a.forEach((d,m)=>{d.forEach((f,h)=>{d.set(h,this._driver.computeStyle(m,h,Cn))})});let l=s.map(d=>{let m=a.get(d.element);return this._buildPlayer(d,new Map,m)}),c=ci(l);return this._playersById.set(t,c),c.onDestroy(()=>this.destroy(t)),this.players.push(c),c}destroy(t){let e=this._getPlayer(t);e.destroy(),this._playersById.delete(t);let i=this.players.indexOf(e);i>=0&&this.players.splice(i,1)}_getPlayer(t){let e=this._playersById.get(t);if(!e)throw eD(t);return e}listen(t,e,i,r){let o=tu(e,"","","");return eu(this._getPlayer(t),i,o,r),()=>{}}command(t,e,i,r){if(i=="register"){this.register(t,r[0]);return}if(i=="create"){let s=r[0]||{};this.create(t,e,s);return}let o=this._getPlayer(t);switch(i){case"play":o.play();break;case"pause":o.pause();break;case"reset":o.reset();break;case"restart":o.restart();break;case"finish":o.finish();break;case"init":o.init();break;case"setPosition":o.setPosition(parseFloat(r[0]));break;case"destroy":this.destroy(t);break}}},_D="ng-animate-queued",j1=".ng-animate-queued",Ig="ng-animate-disabled",z1=".ng-animate-disabled",H1="ng-star-inserted",U1=".ng-star-inserted",$1=[],kD={namespaceId:"",setForRemoval:!1,setForMove:!1,hasAnimation:!1,removedBeforeQueried:!1},G1={namespaceId:"",setForMove:!1,setForRemoval:!1,hasAnimation:!1,removedBeforeQueried:!0},xn="__ng_removed",Fa=class{namespaceId;value;options;get params(){return this.options.params}constructor(t,e=""){this.namespaceId=e;let i=t&&t.hasOwnProperty("value"),r=i?t.value:t;if(this.value=q1(r),i){let o=t,{value:s}=o,a=Am(o,["value"]);this.options=a}else this.options={};this.options.params||(this.options.params={})}absorbOptions(t){let e=t.params;if(e){let i=this.options.params;Object.keys(e).forEach(r=>{i[r]==null&&(i[r]=e[r])})}}},Oa="void",Mg=new Fa(Oa),Vg=class{id;hostElement;_engine;players=[];_triggers=new Map;_queue=[];_elementListeners=new Map;_hostClassName;constructor(t,e,i){this.id=t,this.hostElement=e,this._engine=i,this._hostClassName="ng-tns-"+t,cn(e,this._hostClassName)}listen(t,e,i,r){if(!this._triggers.has(e))throw tD(i,e);if(i==null||i.length==0)throw nD(e);if(!Q1(i))throw iD(i,e);let o=Ft(this._elementListeners,t,[]),s={name:e,phase:i,callback:r};o.push(s);let a=Ft(this._engine.statesByElement,t,new Map);return a.has(e)||(cn(t,Ta),cn(t,Ta+"-"+e),a.set(e,Mg)),()=>{this._engine.afterFlush(()=>{let l=o.indexOf(s);l>=0&&o.splice(l,1),this._triggers.has(e)||a.delete(e)})}}register(t,e){return this._triggers.has(t)?!1:(this._triggers.set(t,e),!0)}_getTrigger(t){let e=this._triggers.get(t);if(!e)throw rD(t);return e}trigger(t,e,i,r=!0){let o=this._getTrigger(e),s=new Pa(this.id,e,t),a=this._engine.statesByElement.get(t);a||(cn(t,Ta),cn(t,Ta+"-"+e),this._engine.statesByElement.set(t,a=new Map));let l=a.get(e),c=new Fa(i,this.id);if(!(i&&i.hasOwnProperty("value"))&&l&&c.absorbOptions(l.options),a.set(e,c),l||(l=Mg),!(c.value===Oa)&&l.value===c.value){if(!Z1(l.params,c.params)){let b=[],C=o.matchStyles(l.value,l.params,b),x=o.matchStyles(c.value,c.params,b);b.length?this._engine.reportError(b):this._engine.afterFlush(()=>{Ui(t,C),Dn(t,x)})}return}let f=Ft(this._engine.playersByElement,t,[]);f.forEach(b=>{b.namespaceId==this.id&&b.triggerName==e&&b.queued&&b.destroy()});let h=o.matchTransition(l.value,c.value,t,c.params),p=!1;if(!h){if(!r)return;h=o.fallbackTransition,p=!0}return this._engine.totalQueuedPlayers++,this._queue.push({element:t,triggerName:e,transition:h,fromState:l,toState:c,player:s,isFallbackTransition:p}),p||(cn(t,_D),s.onStart(()=>{qo(t,_D)})),s.onDone(()=>{let b=this.players.indexOf(s);b>=0&&this.players.splice(b,1);let C=this._engine.playersByElement.get(t);if(C){let x=C.indexOf(s);x>=0&&C.splice(x,1)}}),this.players.push(s),f.push(s),s}deregister(t){this._triggers.delete(t),this._engine.statesByElement.forEach(e=>e.delete(t)),this._elementListeners.forEach((e,i)=>{this._elementListeners.set(i,e.filter(r=>r.name!=t))})}clearElementCache(t){this._engine.statesByElement.delete(t),this._elementListeners.delete(t);let e=this._engine.playersByElement.get(t);e&&(e.forEach(i=>i.destroy()),this._engine.playersByElement.delete(t))}_signalRemovalForInnerTriggers(t,e){let i=this._engine.driver.query(t,Aa,!0);i.forEach(r=>{if(r[xn])return;let o=this._engine.fetchNamespacesByElement(r);o.size?o.forEach(s=>s.triggerLeaveAnimation(r,e,!1,!0)):this.clearElementCache(r)}),this._engine.afterFlushAnimationsDone(()=>i.forEach(r=>this.clearElementCache(r)))}triggerLeaveAnimation(t,e,i,r){let o=this._engine.statesByElement.get(t),s=new Map;if(o){let a=[];if(o.forEach((l,c)=>{if(s.set(c,l.value),this._triggers.has(c)){let d=this.trigger(t,c,Oa,r);d&&a.push(d)}}),a.length)return this._engine.markElementAsRemoved(this.id,t,!0,e,s),i&&ci(a).onDone(()=>this._engine.processLeaveNode(t)),!0}return!1}prepareLeaveAnimationListeners(t){let e=this._elementListeners.get(t),i=this._engine.statesByElement.get(t);if(e&&i){let r=new Set;e.forEach(o=>{let s=o.name;if(r.has(s))return;r.add(s);let l=this._triggers.get(s).fallbackTransition,c=i.get(s)||Mg,d=new Fa(Oa),m=new Pa(this.id,s,t);this._engine.totalQueuedPlayers++,this._queue.push({element:t,triggerName:s,transition:l,fromState:c,toState:d,player:m,isFallbackTransition:!0})})}}removeNode(t,e){let i=this._engine;if(t.childElementCount&&this._signalRemovalForInnerTriggers(t,e),this.triggerLeaveAnimation(t,e,!0))return;let r=!1;if(i.totalAnimations){let o=i.players.length?i.playersByQueriedElement.get(t):[];if(o&&o.length)r=!0;else{let s=t;for(;s=s.parentNode;)if(i.statesByElement.get(s)){r=!0;break}}}if(this.prepareLeaveAnimationListeners(t),r)i.markElementAsRemoved(this.id,t,!1,e);else{let o=t[xn];(!o||o===kD)&&(i.afterFlush(()=>this.clearElementCache(t)),i.destroyInnerAnimations(t),i._onRemovalComplete(t,e))}}insertNode(t,e){cn(t,this._hostClassName)}drainQueuedTransitions(t){let e=[];return this._queue.forEach(i=>{let r=i.player;if(r.destroyed)return;let o=i.element,s=this._elementListeners.get(o);s&&s.forEach(a=>{if(a.name==i.triggerName){let l=tu(o,i.triggerName,i.fromState.value,i.toState.value);l._data=t,eu(i.player,a.phase,l,a.callback)}}),r.markedForDestroy?this._engine.afterFlush(()=>{r.destroy()}):e.push(i)}),this._queue=[],e.sort((i,r)=>{let o=i.transition.ast.depCount,s=r.transition.ast.depCount;return o==0||s==0?o-s:this._engine.driver.containsElement(i.element,r.element)?1:-1})}destroy(t){this.players.forEach(e=>e.destroy()),this._signalRemovalForInnerTriggers(this.hostElement,t)}},jg=class{bodyNode;driver;_normalizer;players=[];newHostElements=new Map;playersByElement=new Map;playersByQueriedElement=new Map;statesByElement=new Map;disabledNodes=new Set;totalAnimations=0;totalQueuedPlayers=0;_namespaceLookup={};_namespaceList=[];_flushFns=[];_whenQuietFns=[];namespacesByHostElement=new Map;collectedEnterElements=[];collectedLeaveElements=[];onRemovalComplete=(t,e)=>{};_onRemovalComplete(t,e){this.onRemovalComplete(t,e)}constructor(t,e,i){this.bodyNode=t,this.driver=e,this._normalizer=i}get queuedPlayers(){let t=[];return this._namespaceList.forEach(e=>{e.players.forEach(i=>{i.queued&&t.push(i)})}),t}createNamespace(t,e){let i=new Vg(t,e,this);return this.bodyNode&&this.driver.containsElement(this.bodyNode,e)?this._balanceNamespaceList(i,e):(this.newHostElements.set(e,i),this.collectEnterElement(e)),this._namespaceLookup[t]=i}_balanceNamespaceList(t,e){let i=this._namespaceList,r=this.namespacesByHostElement;if(i.length-1>=0){let s=!1,a=this.driver.getParentElement(e);for(;a;){let l=r.get(a);if(l){let c=i.indexOf(l);i.splice(c+1,0,t),s=!0;break}a=this.driver.getParentElement(a)}s||i.unshift(t)}else i.push(t);return r.set(e,t),t}register(t,e){let i=this._namespaceLookup[t];return i||(i=this.createNamespace(t,e)),i}registerTrigger(t,e,i){let r=this._namespaceLookup[t];r&&r.register(e,i)&&this.totalAnimations++}destroy(t,e){t&&(this.afterFlush(()=>{}),this.afterFlushAnimationsDone(()=>{let i=this._fetchNamespace(t);this.namespacesByHostElement.delete(i.hostElement);let r=this._namespaceList.indexOf(i);r>=0&&this._namespaceList.splice(r,1),i.destroy(e),delete this._namespaceLookup[t]}))}_fetchNamespace(t){return this._namespaceLookup[t]}fetchNamespacesByElement(t){let e=new Set,i=this.statesByElement.get(t);if(i){for(let r of i.values())if(r.namespaceId){let o=this._fetchNamespace(r.namespaceId);o&&e.add(o)}}return e}trigger(t,e,i,r){if(cu(e)){let o=this._fetchNamespace(t);if(o)return o.trigger(e,i,r),!0}return!1}insertNode(t,e,i,r){if(!cu(e))return;let o=e[xn];if(o&&o.setForRemoval){o.setForRemoval=!1,o.setForMove=!0;let s=this.collectedLeaveElements.indexOf(e);s>=0&&this.collectedLeaveElements.splice(s,1)}if(t){let s=this._fetchNamespace(t);s&&s.insertNode(e,i)}r&&this.collectEnterElement(e)}collectEnterElement(t){this.collectedEnterElements.push(t)}markElementAsDisabled(t,e){e?this.disabledNodes.has(t)||(this.disabledNodes.add(t),cn(t,Ig)):this.disabledNodes.has(t)&&(this.disabledNodes.delete(t),qo(t,Ig))}removeNode(t,e,i){if(cu(e)){let r=t?this._fetchNamespace(t):null;r?r.removeNode(e,i):this.markElementAsRemoved(t,e,!1,i);let o=this.namespacesByHostElement.get(e);o&&o.id!==t&&o.removeNode(e,i)}else this._onRemovalComplete(e,i)}markElementAsRemoved(t,e,i,r,o){this.collectedLeaveElements.push(e),e[xn]={namespaceId:t,setForRemoval:r,hasAnimation:i,removedBeforeQueried:!1,previousTriggersValues:o}}listen(t,e,i,r,o){return cu(e)?this._fetchNamespace(t).listen(e,i,r,o):()=>{}}_buildInstruction(t,e,i,r,o){return t.transition.build(this.driver,t.element,t.fromState.value,t.toState.value,i,r,t.fromState.options,t.toState.options,e,o)}destroyInnerAnimations(t){let e=this.driver.query(t,Aa,!0);e.forEach(i=>this.destroyActiveAnimationsForElement(i)),this.playersByQueriedElement.size!=0&&(e=this.driver.query(t,ru,!0),e.forEach(i=>this.finishActiveQueriedAnimationOnElement(i)))}destroyActiveAnimationsForElement(t){let e=this.playersByElement.get(t);e&&e.forEach(i=>{i.queued?i.markedForDestroy=!0:i.destroy()})}finishActiveQueriedAnimationOnElement(t){let e=this.playersByQueriedElement.get(t);e&&e.forEach(i=>i.finish())}whenRenderingDone(){return new Promise(t=>{if(this.players.length)return ci(this.players).onDone(()=>t());t()})}processLeaveNode(t){let e=t[xn];if(e&&e.setForRemoval){if(t[xn]=kD,e.namespaceId){this.destroyInnerAnimations(t);let i=this._fetchNamespace(e.namespaceId);i&&i.clearElementCache(t)}this._onRemovalComplete(t,e.setForRemoval)}t.classList?.contains(Ig)&&this.markElementAsDisabled(t,!1),this.driver.query(t,z1,!0).forEach(i=>{this.markElementAsDisabled(i,!1)})}flush(t=-1){let e=[];if(this.newHostElements.size&&(this.newHostElements.forEach((i,r)=>this._balanceNamespaceList(i,r)),this.newHostElements.clear()),this.totalAnimations&&this.collectedEnterElements.length)for(let i=0;i<this.collectedEnterElements.length;i++){let r=this.collectedEnterElements[i];cn(r,H1)}if(this._namespaceList.length&&(this.totalQueuedPlayers||this.collectedLeaveElements.length)){let i=[];try{e=this._flushAnimations(i,t)}finally{for(let r=0;r<i.length;r++)i[r]()}}else for(let i=0;i<this.collectedLeaveElements.length;i++){let r=this.collectedLeaveElements[i];this.processLeaveNode(r)}if(this.totalQueuedPlayers=0,this.collectedEnterElements.length=0,this.collectedLeaveElements.length=0,this._flushFns.forEach(i=>i()),this._flushFns=[],this._whenQuietFns.length){let i=this._whenQuietFns;this._whenQuietFns=[],e.length?ci(e).onDone(()=>{i.forEach(r=>r())}):i.forEach(r=>r())}}reportError(t){throw oD(t)}_flushAnimations(t,e){let i=new Na,r=[],o=new Map,s=[],a=new Map,l=new Map,c=new Map,d=new Set;this.disabledNodes.forEach(L=>{d.add(L);let Q=this.driver.query(L,j1,!0);for(let X=0;X<Q.length;X++)d.add(Q[X])});let m=this.bodyNode,f=Array.from(this.statesByElement.keys()),h=yD(f,this.collectedEnterElements),p=new Map,b=0;h.forEach((L,Q)=>{let X=Dg+b++;p.set(Q,X),L.forEach(we=>cn(we,X))});let C=[],x=new Set,ee=new Set;for(let L=0;L<this.collectedLeaveElements.length;L++){let Q=this.collectedLeaveElements[L],X=Q[xn];X&&X.setForRemoval&&(C.push(Q),x.add(Q),X.hasAnimation?this.driver.query(Q,U1,!0).forEach(we=>x.add(we)):ee.add(Q))}let ke=new Map,pe=yD(f,Array.from(x));pe.forEach((L,Q)=>{let X=iu+b++;ke.set(Q,X),L.forEach(we=>cn(we,X))}),t.push(()=>{h.forEach((L,Q)=>{let X=p.get(Q);L.forEach(we=>qo(we,X))}),pe.forEach((L,Q)=>{let X=ke.get(Q);L.forEach(we=>qo(we,X))}),C.forEach(L=>{this.processLeaveNode(L)})});let qt=[],pt=[];for(let L=this._namespaceList.length-1;L>=0;L--)this._namespaceList[L].drainQueuedTransitions(e).forEach(X=>{let we=X.player,at=X.element;if(qt.push(we),this.collectedEnterElements.length){let gt=at[xn];if(gt&&gt.setForMove){if(gt.previousTriggersValues&&gt.previousTriggersValues.has(X.triggerName)){let or=gt.previousTriggersValues.get(X.triggerName),Qt=this.statesByElement.get(X.element);if(Qt&&Qt.has(X.triggerName)){let Dl=Qt.get(X.triggerName);Dl.value=or,Qt.set(X.triggerName,Dl)}}we.destroy();return}}let Tn=!m||!this.driver.containsElement(m,at),Lt=ke.get(at),yi=p.get(at),je=this._buildInstruction(X,i,yi,Lt,Tn);if(je.errors&&je.errors.length){pt.push(je);return}if(Tn){we.onStart(()=>Ui(at,je.fromStyles)),we.onDestroy(()=>Dn(at,je.toStyles)),r.push(we);return}if(X.isFallbackTransition){we.onStart(()=>Ui(at,je.fromStyles)),we.onDestroy(()=>Dn(at,je.toStyles)),r.push(we);return}let pv=[];je.timelines.forEach(gt=>{gt.stretchStartingKeyframe=!0,this.disabledNodes.has(gt.element)||pv.push(gt)}),je.timelines=pv,i.append(at,je.timelines);let bk={instruction:je,player:we,element:at};s.push(bk),je.queriedElements.forEach(gt=>Ft(a,gt,[]).push(we)),je.preStyleProps.forEach((gt,or)=>{if(gt.size){let Qt=l.get(or);Qt||l.set(or,Qt=new Set),gt.forEach((Dl,Tm)=>Qt.add(Tm))}}),je.postStyleProps.forEach((gt,or)=>{let Qt=c.get(or);Qt||c.set(or,Qt=new Set),gt.forEach((Dl,Tm)=>Qt.add(Tm))})});if(pt.length){let L=[];pt.forEach(Q=>{L.push(sD(Q.triggerName,Q.errors))}),qt.forEach(Q=>Q.destroy()),this.reportError(L)}let Ve=new Map,Mt=new Map;s.forEach(L=>{let Q=L.element;i.has(Q)&&(Mt.set(Q,Q),this._beforeAnimationBuild(L.player.namespaceId,L.instruction,Ve))}),r.forEach(L=>{let Q=L.element;this._getPreviousPlayers(Q,!1,L.namespaceId,L.triggerName,null).forEach(we=>{Ft(Ve,Q,[]).push(we),we.destroy()})});let ir=C.filter(L=>wD(L,l,c)),to=new Map;bD(to,this.driver,ee,c,Cn).forEach(L=>{wD(L,l,c)&&ir.push(L)});let rr=new Map;h.forEach((L,Q)=>{bD(rr,this.driver,new Set(L),l,Ma)}),ir.forEach(L=>{let Q=to.get(L),X=rr.get(L);to.set(L,new Map([...Q?.entries()??[],...X?.entries()??[]]))});let Mm=[],hv=[],fv={};s.forEach(L=>{let{element:Q,player:X,instruction:we}=L;if(i.has(Q)){if(d.has(Q)){X.onDestroy(()=>Dn(Q,we.toStyles)),X.disabled=!0,X.overrideTotalTime(we.totalTime),r.push(X);return}let at=fv;if(Mt.size>1){let Lt=Q,yi=[];for(;Lt=Lt.parentNode;){let je=Mt.get(Lt);if(je){at=je;break}yi.push(Lt)}yi.forEach(je=>Mt.set(je,at))}let Tn=this._buildAnimation(X.namespaceId,we,Ve,o,rr,to);if(X.setRealPlayer(Tn),at===fv)Mm.push(X);else{let Lt=this.playersByElement.get(at);Lt&&Lt.length&&(X.parentPlayer=ci(Lt)),r.push(X)}}else Ui(Q,we.fromStyles),X.onDestroy(()=>Dn(Q,we.toStyles)),hv.push(X),d.has(Q)&&r.push(X)}),hv.forEach(L=>{let Q=o.get(L.element);if(Q&&Q.length){let X=ci(Q);L.setRealPlayer(X)}}),r.forEach(L=>{L.parentPlayer?L.syncPlayerEvents(L.parentPlayer):L.destroy()});for(let L=0;L<C.length;L++){let Q=C[L],X=Q[xn];if(qo(Q,iu),X&&X.hasAnimation)continue;let we=[];if(a.size){let Tn=a.get(Q);Tn&&Tn.length&&we.push(...Tn);let Lt=this.driver.query(Q,ru,!0);for(let yi=0;yi<Lt.length;yi++){let je=a.get(Lt[yi]);je&&je.length&&we.push(...je)}}let at=we.filter(Tn=>!Tn.destroyed);at.length?K1(this,Q,at):this.processLeaveNode(Q)}return C.length=0,Mm.forEach(L=>{this.players.push(L),L.onDone(()=>{L.destroy();let Q=this.players.indexOf(L);this.players.splice(Q,1)}),L.play()}),Mm}afterFlush(t){this._flushFns.push(t)}afterFlushAnimationsDone(t){this._whenQuietFns.push(t)}_getPreviousPlayers(t,e,i,r,o){let s=[];if(e){let a=this.playersByQueriedElement.get(t);a&&(s=a)}else{let a=this.playersByElement.get(t);if(a){let l=!o||o==Oa;a.forEach(c=>{c.queued||!l&&c.triggerName!=r||s.push(c)})}}return(i||r)&&(s=s.filter(a=>!(i&&i!=a.namespaceId||r&&r!=a.triggerName))),s}_beforeAnimationBuild(t,e,i){let r=e.triggerName,o=e.element,s=e.isRemovalTransition?void 0:t,a=e.isRemovalTransition?void 0:r;for(let l of e.timelines){let c=l.element,d=c!==o,m=Ft(i,c,[]);this._getPreviousPlayers(c,d,s,a,e.toState).forEach(h=>{let p=h.getRealPlayer();p.beforeDestroy&&p.beforeDestroy(),h.destroy(),m.push(h)})}Ui(o,e.fromStyles)}_buildAnimation(t,e,i,r,o,s){let a=e.triggerName,l=e.element,c=[],d=new Set,m=new Set,f=e.timelines.map(p=>{let b=p.element;d.add(b);let C=b[xn];if(C&&C.removedBeforeQueried)return new li(p.duration,p.delay);let x=b!==l,ee=Y1((i.get(b)||$1).map(Ve=>Ve.getRealPlayer())).filter(Ve=>{let Mt=Ve;return Mt.element?Mt.element===b:!1}),ke=o.get(b),pe=s.get(b),qt=vg(this._normalizer,p.keyframes,ke,pe),pt=this._buildPlayer(p,qt,ee);if(p.subTimeline&&r&&m.add(b),x){let Ve=new Pa(t,a,b);Ve.setRealPlayer(pt),c.push(Ve)}return pt});c.forEach(p=>{Ft(this.playersByQueriedElement,p.element,[]).push(p),p.onDone(()=>W1(this.playersByQueriedElement,p.element,p))}),d.forEach(p=>cn(p,xg));let h=ci(f);return h.onDestroy(()=>{d.forEach(p=>qo(p,xg)),Dn(l,e.toStyles)}),m.forEach(p=>{Ft(r,p,[]).push(h)}),h}_buildPlayer(t,e,i){return e.length>0?this.driver.animate(t.element,e,t.duration,t.delay,t.easing,i):new li(t.duration,t.delay)}},Pa=class{namespaceId;triggerName;element;_player=new li;_containsRealPlayer=!1;_queuedCallbacks=new Map;destroyed=!1;parentPlayer=null;markedForDestroy=!1;disabled=!1;queued=!0;totalTime=0;constructor(t,e,i){this.namespaceId=t,this.triggerName=e,this.element=i}setRealPlayer(t){this._containsRealPlayer||(this._player=t,this._queuedCallbacks.forEach((e,i)=>{e.forEach(r=>eu(t,i,void 0,r))}),this._queuedCallbacks.clear(),this._containsRealPlayer=!0,this.overrideTotalTime(t.totalTime),this.queued=!1)}getRealPlayer(){return this._player}overrideTotalTime(t){this.totalTime=t}syncPlayerEvents(t){let e=this._player;e.triggerCallback&&t.onStart(()=>e.triggerCallback("start")),t.onDone(()=>this.finish()),t.onDestroy(()=>this.destroy())}_queueEvent(t,e){Ft(this._queuedCallbacks,t,[]).push(e)}onDone(t){this.queued&&this._queueEvent("done",t),this._player.onDone(t)}onStart(t){this.queued&&this._queueEvent("start",t),this._player.onStart(t)}onDestroy(t){this.queued&&this._queueEvent("destroy",t),this._player.onDestroy(t)}init(){this._player.init()}hasStarted(){return this.queued?!1:this._player.hasStarted()}play(){!this.queued&&this._player.play()}pause(){!this.queued&&this._player.pause()}restart(){!this.queued&&this._player.restart()}finish(){this._player.finish()}destroy(){this.destroyed=!0,this._player.destroy()}reset(){!this.queued&&this._player.reset()}setPosition(t){this.queued||this._player.setPosition(t)}getPosition(){return this.queued?0:this._player.getPosition()}triggerCallback(t){let e=this._player;e.triggerCallback&&e.triggerCallback(t)}};function W1(n,t,e){let i=n.get(t);if(i){if(i.length){let r=i.indexOf(e);i.splice(r,1)}i.length==0&&n.delete(t)}return i}function q1(n){return n??null}function cu(n){return n&&n.nodeType===1}function Q1(n){return n=="start"||n=="done"}function vD(n,t){let e=n.style.display;return n.style.display=t??"none",e}function bD(n,t,e,i,r){let o=[];e.forEach(l=>o.push(vD(l)));let s=[];i.forEach((l,c)=>{let d=new Map;l.forEach(m=>{let f=t.computeStyle(c,m,r);d.set(m,f),(!f||f.length==0)&&(c[xn]=G1,s.push(c))}),n.set(c,d)});let a=0;return e.forEach(l=>vD(l,o[a++])),s}function yD(n,t){let e=new Map;if(n.forEach(a=>e.set(a,[])),t.length==0)return e;let i=1,r=new Set(t),o=new Map;function s(a){if(!a)return i;let l=o.get(a);if(l)return l;let c=a.parentNode;return e.has(c)?l=c:r.has(c)?l=i:l=s(c),o.set(a,l),l}return t.forEach(a=>{let l=s(a);l!==i&&e.get(l).push(a)}),e}function cn(n,t){n.classList?.add(t)}function qo(n,t){n.classList?.remove(t)}function K1(n,t,e){ci(e).onDone(()=>n.processLeaveNode(t))}function Y1(n){let t=[];return SD(n,t),t}function SD(n,t){for(let e=0;e<n.length;e++){let i=n[e];i instanceof $o?SD(i.players,t):t.push(i)}}function Z1(n,t){let e=Object.keys(n),i=Object.keys(t);if(e.length!=i.length)return!1;for(let r=0;r<e.length;r++){let o=e[r];if(!t.hasOwnProperty(o)||n[o]!==t[o])return!1}return!0}function wD(n,t,e){let i=e.get(n);if(!i)return!1;let r=t.get(n);return r?i.forEach(o=>r.add(o)):t.set(n,i),e.delete(n),!0}var Qo=class{_driver;_normalizer;_transitionEngine;_timelineEngine;_triggerCache={};onRemovalComplete=(t,e)=>{};constructor(t,e,i){this._driver=e,this._normalizer=i,this._transitionEngine=new jg(t.body,e,i),this._timelineEngine=new Bg(t.body,e,i),this._transitionEngine.onRemovalComplete=(r,o)=>this.onRemovalComplete(r,o)}registerTrigger(t,e,i,r,o){let s=t+"-"+r,a=this._triggerCache[s];if(!a){let l=[],c=[],d=DD(this._driver,o,l,c);if(l.length)throw YC(r,l);a=L1(r,d,this._normalizer),this._triggerCache[s]=a}this._transitionEngine.registerTrigger(e,r,a)}register(t,e){this._transitionEngine.register(t,e)}destroy(t,e){this._transitionEngine.destroy(t,e)}onInsert(t,e,i,r){this._transitionEngine.insertNode(t,e,i,r)}onRemove(t,e,i){this._transitionEngine.removeNode(t,e,i)}disableAnimations(t,e){this._transitionEngine.markElementAsDisabled(t,e)}process(t,e,i,r){if(i.charAt(0)=="@"){let[o,s]=bg(i),a=r;this._timelineEngine.command(o,e,s,a)}else this._transitionEngine.trigger(t,e,i,r)}listen(t,e,i,r,o){if(i.charAt(0)=="@"){let[s,a]=bg(i);return this._timelineEngine.listen(s,e,a,o)}return this._transitionEngine.listen(t,e,i,r,o)}flush(t=-1){this._transitionEngine.flush(t)}get players(){return[...this._transitionEngine.players,...this._timelineEngine.players]}whenRenderingDone(){return this._transitionEngine.whenRenderingDone()}afterFlushAnimationsDone(t){this._transitionEngine.afterFlushAnimationsDone(t)}};function X1(n,t){let e=null,i=null;return Array.isArray(t)&&t.length?(e=Tg(t[0]),t.length>1&&(i=Tg(t[t.length-1]))):t instanceof Map&&(e=Tg(t)),e||i?new J1(n,e,i):null}var J1=(()=>{class n{_element;_startStyles;_endStyles;static initialStylesByElement=new WeakMap;_state=0;_initialStyles;constructor(e,i,r){this._element=e,this._startStyles=i,this._endStyles=r;let o=n.initialStylesByElement.get(e);o||n.initialStylesByElement.set(e,o=new Map),this._initialStyles=o}start(){this._state<1&&(this._startStyles&&Dn(this._element,this._startStyles,this._initialStyles),this._state=1)}finish(){this.start(),this._state<2&&(Dn(this._element,this._initialStyles),this._endStyles&&(Dn(this._element,this._endStyles),this._endStyles=null),this._state=1)}destroy(){this.finish(),this._state<3&&(n.initialStylesByElement.delete(this._element),this._startStyles&&(Ui(this._element,this._startStyles),this._endStyles=null),this._endStyles&&(Ui(this._element,this._endStyles),this._endStyles=null),Dn(this._element,this._initialStyles),this._state=3)}}return n})();function Tg(n){let t=null;return n.forEach((e,i)=>{eO(i)&&(t=t||new Map,t.set(i,e))}),t}function eO(n){return n==="display"||n==="position"}var gu=class{element;keyframes;options;_specialStyles;_onDoneFns=[];_onStartFns=[];_onDestroyFns=[];_duration;_delay;_initialized=!1;_finished=!1;_started=!1;_destroyed=!1;_finalKeyframe;_originalOnDoneFns=[];_originalOnStartFns=[];domPlayer=null;time=0;parentPlayer=null;currentSnapshot=new Map;constructor(t,e,i,r){this.element=t,this.keyframes=e,this.options=i,this._specialStyles=r,this._duration=i.duration,this._delay=i.delay||0,this.time=this._duration+this._delay}_onFinish(){this._finished||(this._finished=!0,this._onDoneFns.forEach(t=>t()),this._onDoneFns=[])}init(){this._buildPlayer()&&this._preparePlayerBeforeStart()}_buildPlayer(){if(this._initialized)return this.domPlayer;this._initialized=!0;let t=this.keyframes,e=this._triggerWebAnimation(this.element,t,this.options);if(!e)return this._onFinish(),null;this.domPlayer=e,this._finalKeyframe=t.length?t[t.length-1]:new Map;let i=()=>this._onFinish();return e.addEventListener("finish",i),this.onDestroy(()=>{e.removeEventListener("finish",i)}),e}_preparePlayerBeforeStart(){this._delay?this._resetDomPlayerState():this.domPlayer?.pause()}_convertKeyframesToObject(t){let e=[];return t.forEach(i=>{e.push(Object.fromEntries(i))}),e}_triggerWebAnimation(t,e,i){let r=this._convertKeyframesToObject(e);try{return t.animate(r,i)}catch(o){return null}}onStart(t){this._originalOnStartFns.push(t),this._onStartFns.push(t)}onDone(t){this._originalOnDoneFns.push(t),this._onDoneFns.push(t)}onDestroy(t){this._onDestroyFns.push(t)}play(){let t=this._buildPlayer();t&&(this.hasStarted()||(this._onStartFns.forEach(e=>e()),this._onStartFns=[],this._started=!0,this._specialStyles&&this._specialStyles.start()),t.play())}pause(){this.init(),this.domPlayer?.pause()}finish(){this.init(),this.domPlayer&&(this._specialStyles&&this._specialStyles.finish(),this._onFinish(),this.domPlayer.finish())}reset(){this._resetDomPlayerState(),this._destroyed=!1,this._finished=!1,this._started=!1,this._onStartFns=this._originalOnStartFns,this._onDoneFns=this._originalOnDoneFns}_resetDomPlayerState(){this.domPlayer?.cancel()}restart(){this.reset(),this.play()}hasStarted(){return this._started}destroy(){this._destroyed||(this._destroyed=!0,this._resetDomPlayerState(),this._onFinish(),this._specialStyles&&this._specialStyles.destroy(),this._onDestroyFns.forEach(t=>t()),this._onDestroyFns=[])}setPosition(t){this.domPlayer||this.init(),this.domPlayer&&(this.domPlayer.currentTime=t*this.time)}getPosition(){return this.domPlayer?+(this.domPlayer.currentTime??0)/this.time:this._initialized?1:0}get totalTime(){return this._delay+this._duration}beforeDestroy(){let t=new Map;this.hasStarted()&&this._finalKeyframe.forEach((i,r)=>{r!=="offset"&&t.set(r,this._finished?i:su(this.element,r))}),this.currentSnapshot=t}triggerCallback(t){let e=t==="start"?this._onStartFns:this._onDoneFns;e.forEach(i=>i()),e.length=0}},_u=class{validateStyleProperty(t){return!0}validateAnimatableStyleProperty(t){return!0}containsElement(t,e){return yg(t,e)}getParentElement(t){return nu(t)}query(t,e,i){return wg(t,e,i)}computeStyle(t,e,i){return su(t,e)}animate(t,e,i,r,o,s=[]){let a=r==0?"both":"forwards",l={duration:i,delay:r,fill:a};o&&(l.easing=o);let c=new Map,d=s.filter(h=>h instanceof gu);dD(i,r)&&d.forEach(h=>{h.currentSnapshot.forEach((p,b)=>c.set(b,p))});let m=lD(e).map(h=>new Map(h));m=uD(t,m,c);let f=X1(t,m);return new gu(t,m,l,f)}};var du="@",ID="@.disabled",vu=class{namespaceId;delegate;engine;_onDestroy;\u0275type=0;constructor(t,e,i,r){this.namespaceId=t,this.delegate=e,this.engine=i,this._onDestroy=r}get data(){return this.delegate.data}destroyNode(t){this.delegate.destroyNode?.(t)}destroy(){this.engine.destroy(this.namespaceId,this.delegate),this.engine.afterFlushAnimationsDone(()=>{queueMicrotask(()=>{this.delegate.destroy()})}),this._onDestroy?.()}createElement(t,e){return this.delegate.createElement(t,e)}createComment(t){return this.delegate.createComment(t)}createText(t){return this.delegate.createText(t)}appendChild(t,e){this.delegate.appendChild(t,e),this.engine.onInsert(this.namespaceId,e,t,!1)}insertBefore(t,e,i,r=!0){this.delegate.insertBefore(t,e,i),this.engine.onInsert(this.namespaceId,e,t,r)}removeChild(t,e,i,r){if(r){this.delegate.removeChild(t,e,i,r);return}this.parentNode(e)&&this.engine.onRemove(this.namespaceId,e,this.delegate)}selectRootElement(t,e){return this.delegate.selectRootElement(t,e)}parentNode(t){return this.delegate.parentNode(t)}nextSibling(t){return this.delegate.nextSibling(t)}setAttribute(t,e,i,r){this.delegate.setAttribute(t,e,i,r)}removeAttribute(t,e,i){this.delegate.removeAttribute(t,e,i)}addClass(t,e){this.delegate.addClass(t,e)}removeClass(t,e){this.delegate.removeClass(t,e)}setStyle(t,e,i,r){this.delegate.setStyle(t,e,i,r)}removeStyle(t,e,i){this.delegate.removeStyle(t,e,i)}setProperty(t,e,i){e.charAt(0)==du&&e==ID?this.disableAnimations(t,!!i):this.delegate.setProperty(t,e,i)}setValue(t,e){this.delegate.setValue(t,e)}listen(t,e,i,r){return this.delegate.listen(t,e,i,r)}disableAnimations(t,e){this.engine.disableAnimations(t,e)}},zg=class extends vu{factory;constructor(t,e,i,r,o){super(e,i,r,o),this.factory=t,this.namespaceId=e}setProperty(t,e,i){e.charAt(0)==du?e.charAt(1)=="."&&e==ID?(i=i===void 0?!0:!!i,this.disableAnimations(t,i)):this.engine.process(this.namespaceId,t,e.slice(1),i):this.delegate.setProperty(t,e,i)}listen(t,e,i,r){if(e.charAt(0)==du){let o=tO(t),s=e.slice(1),a="";return s.charAt(0)!=du&&([s,a]=nO(s)),this.engine.listen(this.namespaceId,o,s,a,l=>{let c=l._data||-1;this.factory.scheduleListenerCallback(c,i,l)})}return this.delegate.listen(t,e,i,r)}};function tO(n){switch(n){case"body":return document.body;case"document":return document;case"window":return window;default:return n}}function nO(n){let t=n.indexOf("."),e=n.substring(0,t),i=n.slice(t+1);return[e,i]}var bu=class{delegate;engine;_zone;_currentId=0;_microtaskId=1;_animationCallbacksBuffer=[];_rendererCache=new Map;_cdRecurDepth=0;constructor(t,e,i){this.delegate=t,this.engine=e,this._zone=i,e.onRemovalComplete=(r,o)=>{o?.removeChild(null,r)}}createRenderer(t,e){let r=this.delegate.createRenderer(t,e);if(!t||!e?.data?.animation){let c=this._rendererCache,d=c.get(r);if(!d){let m=()=>c.delete(r);d=new vu("",r,this.engine,m),c.set(r,d)}return d}let o=e.id,s=e.id+"-"+this._currentId;this._currentId++,this.engine.register(s,t);let a=c=>{Array.isArray(c)?c.forEach(a):this.engine.registerTrigger(o,s,t,c.name,c)};return e.data.animation.forEach(a),new zg(this,s,r,this.engine)}begin(){this._cdRecurDepth++,this.delegate.begin&&this.delegate.begin()}_scheduleCountTask(){queueMicrotask(()=>{this._microtaskId++})}scheduleListenerCallback(t,e,i){if(t>=0&&t<this._microtaskId){this._zone.run(()=>e(i));return}let r=this._animationCallbacksBuffer;r.length==0&&queueMicrotask(()=>{this._zone.run(()=>{r.forEach(o=>{let[s,a]=o;s(a)}),this._animationCallbacksBuffer=[]})}),r.push([e,i])}end(){this._cdRecurDepth--,this._cdRecurDepth==0&&this._zone.runOutsideAngular(()=>{this._scheduleCountTask(),this.engine.flush(this._microtaskId)}),this.delegate.end&&this.delegate.end()}whenRenderingDone(){return this.engine.whenRenderingDone()}componentReplaced(t){this.engine.flush(),this.delegate.componentReplaced?.(t)}};var rO=(()=>{class n extends Qo{constructor(e,i,r){super(e,i,r)}ngOnDestroy(){this.flush()}static \u0275fac=function(i){return new(i||n)(B(U),B(zr),B(Hr))};static \u0275prov=w({token:n,factory:n.\u0275fac})}return n})();function oO(){return new uu}function sO(){return new bu(u(Da),u(Qo),u(F))}var TD=[{provide:Hr,useFactory:oO},{provide:Qo,useClass:rO},{provide:qe,useFactory:sO}],aO=[{provide:zr,useClass:Hg},{provide:Vi,useValue:"NoopAnimations"},...TD],MD=[{provide:zr,useFactory:()=>new _u},{provide:Vi,useFactory:()=>"BrowserAnimations"},...TD],AD=(()=>{class n{static withConfig(e){return{ngModule:n,providers:e.disableAnimations?aO:MD}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:MD,imports:[Ea]})}return n})();var jD=(()=>{class n{_renderer;_elementRef;onChange=e=>{};onTouched=()=>{};constructor(e,i){this._renderer=e,this._elementRef=i}setProperty(e,i){this._renderer.setProperty(this._elementRef.nativeElement,e,i)}registerOnTouched(e){this.onTouched=e}registerOnChange(e){this.onChange=e}setDisabledState(e){this.setProperty("disabled",e)}static \u0275fac=function(i){return new(i||n)(q(Ke),q(P))};static \u0275dir=T({type:n})}return n})(),lO=(()=>{class n extends jD{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,features:[re]})}return n})(),Ur=new v("");var cO={provide:Ur,useExisting:bt(()=>es),multi:!0};function dO(){let n=Gt()?Gt().getUserAgent():"";return/android (\d+)/.test(n.toLowerCase())}var uO=new v(""),es=(()=>{class n extends jD{_compositionMode;_composing=!1;constructor(e,i,r){super(e,i),this._compositionMode=r,this._compositionMode==null&&(this._compositionMode=!dO())}writeValue(e){let i=e??"";this.setProperty("value",i)}_handleInput(e){(!this._compositionMode||this._compositionMode&&!this._composing)&&this.onChange(e)}_compositionStart(){this._composing=!0}_compositionEnd(e){this._composing=!1,this._compositionMode&&this.onChange(e)}static \u0275fac=function(i){return new(i||n)(q(Ke),q(P),q(uO,8))};static \u0275dir=T({type:n,selectors:[["input","formControlName","",3,"type","checkbox"],["textarea","formControlName",""],["input","formControl","",3,"type","checkbox"],["textarea","formControl",""],["input","ngModel","",3,"type","checkbox"],["textarea","ngModel",""],["","ngDefaultControl",""]],hostBindings:function(i,r){i&1&&V("input",function(s){return r._handleInput(s.target.value)})("blur",function(){return r.onTouched()})("compositionstart",function(){return r._compositionStart()})("compositionend",function(s){return r._compositionEnd(s.target.value)})},standalone:!1,features:[ve([cO]),re]})}return n})();function qg(n){return n==null||Qg(n)===0}function Qg(n){return n==null?null:Array.isArray(n)||typeof n=="string"?n.length:n instanceof Set?n.size:null}var ts=new v(""),Mu=new v(""),mO=/^(?=.{1,254}$)(?=.{1,64}@)[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-zA-Z0-9!#$%&'*+/=?^_`{|}~-]+)*@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$/,Zo=class{static min(t){return hO(t)}static max(t){return fO(t)}static required(t){return pO(t)}static requiredTrue(t){return gO(t)}static email(t){return _O(t)}static minLength(t){return vO(t)}static maxLength(t){return bO(t)}static pattern(t){return yO(t)}static nullValidator(t){return zD()}static compose(t){return qD(t)}static composeAsync(t){return QD(t)}};function hO(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e<n?{min:{min:n,actual:t.value}}:null}}function fO(n){return t=>{if(t.value==null||n==null)return null;let e=parseFloat(t.value);return!isNaN(e)&&e>n?{max:{max:n,actual:t.value}}:null}}function pO(n){return qg(n.value)?{required:!0}:null}function gO(n){return n.value===!0?null:{required:!0}}function _O(n){return qg(n.value)||mO.test(n.value)?null:{email:!0}}function vO(n){return t=>{let e=t.value?.length??Qg(t.value);return e===null||e===0?null:e<n?{minlength:{requiredLength:n,actualLength:e}}:null}}function bO(n){return t=>{let e=t.value?.length??Qg(t.value);return e!==null&&e>n?{maxlength:{requiredLength:n,actualLength:e}}:null}}function yO(n){if(!n)return zD;let t,e;return typeof n=="string"?(e="",n.charAt(0)!=="^"&&(e+="^"),e+=n,n.charAt(n.length-1)!=="$"&&(e+="$"),t=new RegExp(e)):(e=n.toString(),t=n),i=>{if(qg(i.value))return null;let r=i.value;return t.test(r)?null:{pattern:{requiredPattern:e,actualValue:r}}}}function zD(n){return null}function HD(n){return n!=null}function UD(n){return Bo(n)?Yt(n):n}function $D(n){let t={};return n.forEach(e=>{t=e!=null?M(M({},t),e):t}),Object.keys(t).length===0?null:t}function GD(n,t){return t.map(e=>e(n))}function wO(n){return!n.validate}function WD(n){return n.map(t=>wO(t)?t:e=>t.validate(e))}function qD(n){if(!n)return null;let t=n.filter(HD);return t.length==0?null:function(e){return $D(GD(e,t))}}function Kg(n){return n!=null?qD(WD(n)):null}function QD(n){if(!n)return null;let t=n.filter(HD);return t.length==0?null:function(e){let i=GD(e,t).map(UD);return Is(i).pipe(ye($D))}}function Yg(n){return n!=null?QD(WD(n)):null}function RD(n,t){return n===null?[t]:Array.isArray(n)?[...n,t]:[n,t]}function KD(n){return n._rawValidators}function YD(n){return n._rawAsyncValidators}function $g(n){return n?Array.isArray(n)?n:[n]:[]}function wu(n,t){return Array.isArray(n)?n.includes(t):n===t}function OD(n,t){let e=$g(t);return $g(n).forEach(r=>{wu(e,r)||e.push(r)}),e}function ND(n,t){return $g(t).filter(e=>!wu(n,e))}var Cu=class{get value(){return this.control?this.control.value:null}get valid(){return this.control?this.control.valid:null}get invalid(){return this.control?this.control.invalid:null}get pending(){return this.control?this.control.pending:null}get disabled(){return this.control?this.control.disabled:null}get enabled(){return this.control?this.control.enabled:null}get errors(){return this.control?this.control.errors:null}get pristine(){return this.control?this.control.pristine:null}get dirty(){return this.control?this.control.dirty:null}get touched(){return this.control?this.control.touched:null}get status(){return this.control?this.control.status:null}get untouched(){return this.control?this.control.untouched:null}get statusChanges(){return this.control?this.control.statusChanges:null}get valueChanges(){return this.control?this.control.valueChanges:null}get path(){return null}_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators=[];_rawAsyncValidators=[];_setValidators(t){this._rawValidators=t||[],this._composedValidatorFn=Kg(this._rawValidators)}_setAsyncValidators(t){this._rawAsyncValidators=t||[],this._composedAsyncValidatorFn=Yg(this._rawAsyncValidators)}get validator(){return this._composedValidatorFn||null}get asyncValidator(){return this._composedAsyncValidatorFn||null}_onDestroyCallbacks=[];_registerOnDestroy(t){this._onDestroyCallbacks.push(t)}_invokeOnDestroyCallbacks(){this._onDestroyCallbacks.forEach(t=>t()),this._onDestroyCallbacks=[]}reset(t=void 0){this.control?.reset(t)}hasError(t,e){return this.control?this.control.hasError(t,e):!1}getError(t,e){return this.control?this.control.getError(t,e):null}},di=class extends Cu{name;get formDirective(){return null}get path(){return null}},En=class extends Cu{_parent=null;name=null;valueAccessor=null},Du=class{_cd;constructor(t){this._cd=t}get isTouched(){return this._cd?.control?._touched?.(),!!this._cd?.control?.touched}get isUntouched(){return!!this._cd?.control?.untouched}get isPristine(){return this._cd?.control?._pristine?.(),!!this._cd?.control?.pristine}get isDirty(){return!!this._cd?.control?.dirty}get isValid(){return this._cd?.control?._status?.(),!!this._cd?.control?.valid}get isInvalid(){return!!this._cd?.control?.invalid}get isPending(){return!!this._cd?.control?.pending}get isSubmitted(){return this._cd?._submitted?.(),!!this._cd?.submitted}};var Tu=(()=>{class n extends Du{constructor(e){super(e)}static \u0275fac=function(i){return new(i||n)(q(En,2))};static \u0275dir=T({type:n,selectors:[["","formControlName",""],["","ngModel",""],["","formControl",""]],hostVars:14,hostBindings:function(i,r){i&2&&j("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)},standalone:!1,features:[re]})}return n})(),ZD=(()=>{class n extends Du{constructor(e){super(e)}static \u0275fac=function(i){return new(i||n)(q(di,10))};static \u0275dir=T({type:n,selectors:[["","formGroupName",""],["","formArrayName",""],["","ngModelGroup",""],["","formGroup",""],["","formArray",""],["form",3,"ngNoForm",""],["","ngForm",""]],hostVars:16,hostBindings:function(i,r){i&2&&j("ng-untouched",r.isUntouched)("ng-touched",r.isTouched)("ng-pristine",r.isPristine)("ng-dirty",r.isDirty)("ng-valid",r.isValid)("ng-invalid",r.isInvalid)("ng-pending",r.isPending)("ng-submitted",r.isSubmitted)},standalone:!1,features:[re]})}return n})();var La="VALID",yu="INVALID",Ko="PENDING",Ba="DISABLED",$i=class{},xu=class extends $i{value;source;constructor(t,e){super(),this.value=t,this.source=e}},ja=class extends $i{pristine;source;constructor(t,e){super(),this.pristine=t,this.source=e}},za=class extends $i{touched;source;constructor(t,e){super(),this.touched=t,this.source=e}},Yo=class extends $i{status;source;constructor(t,e){super(),this.status=t,this.source=e}},Eu=class extends $i{source;constructor(t){super(),this.source=t}},Ua=class extends $i{source;constructor(t){super(),this.source=t}};function Zg(n){return(Au(n)?n.validators:n)||null}function CO(n){return Array.isArray(n)?Kg(n):n||null}function Xg(n,t){return(Au(t)?t.asyncValidators:n)||null}function DO(n){return Array.isArray(n)?Yg(n):n||null}function Au(n){return n!=null&&!Array.isArray(n)&&typeof n=="object"}function XD(n,t,e){let i=n.controls;if(!(t?Object.keys(i):i).length)throw new D(1e3,"");if(!i[e])throw new D(1001,"")}function JD(n,t,e){n._forEachChild((i,r)=>{if(e[r]===void 0)throw new D(-1002,"")})}var Xo=class{_pendingDirty=!1;_hasOwnPendingAsyncValidator=null;_pendingTouched=!1;_onCollectionChange=()=>{};_updateOn;_parent=null;_asyncValidationSubscription;_composedValidatorFn;_composedAsyncValidatorFn;_rawValidators;_rawAsyncValidators;value;constructor(t,e){this._assignValidators(t),this._assignAsyncValidators(e)}get validator(){return this._composedValidatorFn}set validator(t){this._rawValidators=this._composedValidatorFn=t}get asyncValidator(){return this._composedAsyncValidatorFn}set asyncValidator(t){this._rawAsyncValidators=this._composedAsyncValidatorFn=t}get parent(){return this._parent}get status(){return sn(this.statusReactive)}set status(t){sn(()=>this.statusReactive.set(t))}_status=wn(()=>this.statusReactive());statusReactive=Oe(void 0);get valid(){return this.status===La}get invalid(){return this.status===yu}get pending(){return this.status===Ko}get disabled(){return this.status===Ba}get enabled(){return this.status!==Ba}errors;get pristine(){return sn(this.pristineReactive)}set pristine(t){sn(()=>this.pristineReactive.set(t))}_pristine=wn(()=>this.pristineReactive());pristineReactive=Oe(!0);get dirty(){return!this.pristine}get touched(){return sn(this.touchedReactive)}set touched(t){sn(()=>this.touchedReactive.set(t))}_touched=wn(()=>this.touchedReactive());touchedReactive=Oe(!1);get untouched(){return!this.touched}_events=new k;events=this._events.asObservable();valueChanges;statusChanges;get updateOn(){return this._updateOn?this._updateOn:this.parent?this.parent.updateOn:"change"}setValidators(t){this._assignValidators(t)}setAsyncValidators(t){this._assignAsyncValidators(t)}addValidators(t){this.setValidators(OD(t,this._rawValidators))}addAsyncValidators(t){this.setAsyncValidators(OD(t,this._rawAsyncValidators))}removeValidators(t){this.setValidators(ND(t,this._rawValidators))}removeAsyncValidators(t){this.setAsyncValidators(ND(t,this._rawAsyncValidators))}hasValidator(t){return wu(this._rawValidators,t)}hasAsyncValidator(t){return wu(this._rawAsyncValidators,t)}clearValidators(){this.validator=null}clearAsyncValidators(){this.asyncValidator=null}markAsTouched(t={}){let e=this.touched===!1;this.touched=!0;let i=t.sourceControl??this;t.onlySelf||this._parent?.markAsTouched(be(M({},t),{sourceControl:i})),e&&t.emitEvent!==!1&&this._events.next(new za(!0,i))}markAllAsDirty(t={}){this.markAsDirty({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsDirty(t))}markAllAsTouched(t={}){this.markAsTouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:this}),this._forEachChild(e=>e.markAllAsTouched(t))}markAsUntouched(t={}){let e=this.touched===!0;this.touched=!1,this._pendingTouched=!1;let i=t.sourceControl??this;this._forEachChild(r=>{r.markAsUntouched({onlySelf:!0,emitEvent:t.emitEvent,sourceControl:i})}),t.onlySelf||this._parent?._updateTouched(t,i),e&&t.emitEvent!==!1&&this._events.next(new za(!1,i))}markAsDirty(t={}){let e=this.pristine===!0;this.pristine=!1;let i=t.sourceControl??this;t.onlySelf||this._parent?.markAsDirty(be(M({},t),{sourceControl:i})),e&&t.emitEvent!==!1&&this._events.next(new ja(!1,i))}markAsPristine(t={}){let e=this.pristine===!1;this.pristine=!0,this._pendingDirty=!1;let i=t.sourceControl??this;this._forEachChild(r=>{r.markAsPristine({onlySelf:!0,emitEvent:t.emitEvent})}),t.onlySelf||this._parent?._updatePristine(t,i),e&&t.emitEvent!==!1&&this._events.next(new ja(!0,i))}markAsPending(t={}){this.status=Ko;let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new Yo(this.status,e)),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.markAsPending(be(M({},t),{sourceControl:e}))}disable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=Ba,this.errors=null,this._forEachChild(r=>{r.disable(be(M({},t),{onlySelf:!0}))}),this._updateValue();let i=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new xu(this.value,i)),this._events.next(new Yo(this.status,i)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),this._updateAncestors(be(M({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(r=>r(!0))}enable(t={}){let e=this._parentMarkedDirty(t.onlySelf);this.status=La,this._forEachChild(i=>{i.enable(be(M({},t),{onlySelf:!0}))}),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent}),this._updateAncestors(be(M({},t),{skipPristineCheck:e}),this),this._onDisabledChange.forEach(i=>i(!1))}_updateAncestors(t,e){t.onlySelf||(this._parent?.updateValueAndValidity(t),t.skipPristineCheck||this._parent?._updatePristine({},e),this._parent?._updateTouched({},e))}setParent(t){this._parent=t}getRawValue(){return this.value}updateValueAndValidity(t={}){if(this._setInitialStatus(),this._updateValue(),this.enabled){let i=this._cancelExistingSubscription();this.errors=this._runValidator(),this.status=this._calculateStatus(),(this.status===La||this.status===Ko)&&this._runAsyncValidator(i,t.emitEvent)}let e=t.sourceControl??this;t.emitEvent!==!1&&(this._events.next(new xu(this.value,e)),this._events.next(new Yo(this.status,e)),this.valueChanges.emit(this.value),this.statusChanges.emit(this.status)),t.onlySelf||this._parent?.updateValueAndValidity(be(M({},t),{sourceControl:e}))}_updateTreeValidity(t={emitEvent:!0}){this._forEachChild(e=>e._updateTreeValidity(t)),this.updateValueAndValidity({onlySelf:!0,emitEvent:t.emitEvent})}_setInitialStatus(){this.status=this._allControlsDisabled()?Ba:La}_runValidator(){return this.validator?this.validator(this):null}_runAsyncValidator(t,e){if(this.asyncValidator){this.status=Ko,this._hasOwnPendingAsyncValidator={emitEvent:e!==!1,shouldHaveEmitted:t!==!1};let i=UD(this.asyncValidator(this));this._asyncValidationSubscription=i.subscribe(r=>{this._hasOwnPendingAsyncValidator=null,this.setErrors(r,{emitEvent:e,shouldHaveEmitted:t})})}}_cancelExistingSubscription(){if(this._asyncValidationSubscription){this._asyncValidationSubscription.unsubscribe();let t=(this._hasOwnPendingAsyncValidator?.emitEvent||this._hasOwnPendingAsyncValidator?.shouldHaveEmitted)??!1;return this._hasOwnPendingAsyncValidator=null,t}return!1}setErrors(t,e={}){this.errors=t,this._updateControlsErrors(e.emitEvent!==!1,this,e.shouldHaveEmitted)}get(t){let e=t;return e==null||(Array.isArray(e)||(e=e.split(".")),e.length===0)?null:e.reduce((i,r)=>i&&i._find(r),this)}getError(t,e){let i=e?this.get(e):this;return i?.errors?i.errors[t]:null}hasError(t,e){return!!this.getError(t,e)}get root(){let t=this;for(;t._parent;)t=t._parent;return t}_updateControlsErrors(t,e,i){this.status=this._calculateStatus(),t&&this.statusChanges.emit(this.status),(t||i)&&this._events.next(new Yo(this.status,e)),this._parent&&this._parent._updateControlsErrors(t,e,i)}_initObservables(){this.valueChanges=new z,this.statusChanges=new z}_calculateStatus(){return this._allControlsDisabled()?Ba:this.errors?yu:this._hasOwnPendingAsyncValidator||this._anyControlsHaveStatus(Ko)?Ko:this._anyControlsHaveStatus(yu)?yu:La}_anyControlsHaveStatus(t){return this._anyControls(e=>e.status===t)}_anyControlsDirty(){return this._anyControls(t=>t.dirty)}_anyControlsTouched(){return this._anyControls(t=>t.touched)}_updatePristine(t,e){let i=!this._anyControlsDirty(),r=this.pristine!==i;this.pristine=i,t.onlySelf||this._parent?._updatePristine(t,e),r&&this._events.next(new ja(this.pristine,e))}_updateTouched(t={},e){this.touched=this._anyControlsTouched(),this._events.next(new za(this.touched,e)),t.onlySelf||this._parent?._updateTouched(t,e)}_onDisabledChange=[];_registerOnCollectionChange(t){this._onCollectionChange=t}_setUpdateStrategy(t){Au(t)&&t.updateOn!=null&&(this._updateOn=t.updateOn)}_parentMarkedDirty(t){return!t&&!!this._parent?.dirty&&!this._parent._anyControlsDirty()}_find(t){return null}_assignValidators(t){this._rawValidators=Array.isArray(t)?t.slice():t,this._composedValidatorFn=CO(this._rawValidators)}_assignAsyncValidators(t){this._rawAsyncValidators=Array.isArray(t)?t.slice():t,this._composedAsyncValidatorFn=DO(this._rawAsyncValidators)}},Jo=class extends Xo{constructor(t,e,i){super(Zg(e),Xg(i,e)),this.controls=t,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;registerControl(t,e){return this.controls[t]?this.controls[t]:(this.controls[t]=e,e.setParent(this),e._registerOnCollectionChange(this._onCollectionChange),e)}addControl(t,e,i={}){this.registerControl(t,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}removeControl(t,e={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}setControl(t,e,i={}){this.controls[t]&&this.controls[t]._registerOnCollectionChange(()=>{}),delete this.controls[t],e&&this.registerControl(t,e),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}contains(t){return this.controls.hasOwnProperty(t)&&this.controls[t].enabled}setValue(t,e={}){JD(this,!0,t),Object.keys(t).forEach(i=>{XD(this,!0,i),this.controls[i].setValue(t[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(t,e={}){t!=null&&(Object.keys(t).forEach(i=>{let r=this.controls[i];r&&r.patchValue(t[i],{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(t={},e={}){this._forEachChild((i,r)=>{i.reset(t?t[r]:null,be(M({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Ua(this))}getRawValue(){return this._reduceChildren({},(t,e,i)=>(t[i]=e.getRawValue(),t))}_syncPendingControls(){let t=this._reduceChildren(!1,(e,i)=>i._syncPendingControls()?!0:e);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){Object.keys(this.controls).forEach(e=>{let i=this.controls[e];i&&t(i,e)})}_setUpControls(){this._forEachChild(t=>{t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)})}_updateValue(){this.value=this._reduceValue()}_anyControls(t){for(let[e,i]of Object.entries(this.controls))if(this.contains(e)&&t(i))return!0;return!1}_reduceValue(){let t={};return this._reduceChildren(t,(e,i,r)=>((i.enabled||this.disabled)&&(e[r]=i.value),e))}_reduceChildren(t,e){let i=t;return this._forEachChild((r,o)=>{i=e(i,r,o)}),i}_allControlsDisabled(){for(let t of Object.keys(this.controls))if(this.controls[t].enabled)return!1;return Object.keys(this.controls).length>0||this.disabled}_find(t){return this.controls.hasOwnProperty(t)?this.controls[t]:null}};var Gg=class extends Jo{};var $a=new v("",{factory:()=>Ru}),Ru="always";function ex(n,t){return[...t.path,n]}function ku(n,t,e=Ru){Jg(n,t),t.valueAccessor.writeValue(n.value),(n.disabled||e==="always")&&t.valueAccessor.setDisabledState?.(n.disabled),EO(n,t),SO(n,t),kO(n,t),xO(n,t)}function FD(n,t,e=!0){let i=()=>{};t?.valueAccessor?.registerOnChange(i),t?.valueAccessor?.registerOnTouched(i),Iu(n,t),n&&(t._invokeOnDestroyCallbacks(),n._registerOnCollectionChange(()=>{}))}function Su(n,t){n.forEach(e=>{e.registerOnValidatorChange&&e.registerOnValidatorChange(t)})}function xO(n,t){if(t.valueAccessor.setDisabledState){let e=i=>{t.valueAccessor.setDisabledState(i)};n.registerOnDisabledChange(e),t._registerOnDestroy(()=>{n._unregisterOnDisabledChange(e)})}}function Jg(n,t){let e=KD(n);t.validator!==null?n.setValidators(RD(e,t.validator)):typeof e=="function"&&n.setValidators([e]);let i=YD(n);t.asyncValidator!==null?n.setAsyncValidators(RD(i,t.asyncValidator)):typeof i=="function"&&n.setAsyncValidators([i]);let r=()=>n.updateValueAndValidity();Su(t._rawValidators,r),Su(t._rawAsyncValidators,r)}function Iu(n,t){let e=!1;if(n!==null){if(t.validator!==null){let r=KD(n);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==t.validator);o.length!==r.length&&(e=!0,n.setValidators(o))}}if(t.asyncValidator!==null){let r=YD(n);if(Array.isArray(r)&&r.length>0){let o=r.filter(s=>s!==t.asyncValidator);o.length!==r.length&&(e=!0,n.setAsyncValidators(o))}}}let i=()=>{};return Su(t._rawValidators,i),Su(t._rawAsyncValidators,i),e}function EO(n,t){t.valueAccessor.registerOnChange(e=>{n._pendingValue=e,n._pendingChange=!0,n._pendingDirty=!0,n.updateOn==="change"&&tx(n,t)})}function kO(n,t){t.valueAccessor.registerOnTouched(()=>{n._pendingTouched=!0,n.updateOn==="blur"&&n._pendingChange&&tx(n,t),n.updateOn!=="submit"&&n.markAsTouched()})}function tx(n,t){n._pendingDirty&&n.markAsDirty(),n.setValue(n._pendingValue,{emitModelToViewChange:!1}),t.viewToModelUpdate(n._pendingValue),n._pendingChange=!1}function SO(n,t){let e=(i,r)=>{t.valueAccessor.writeValue(i),r&&t.viewToModelUpdate(i)};n.registerOnChange(e),t._registerOnDestroy(()=>{n._unregisterOnChange(e)})}function nx(n,t){n==null,Jg(n,t)}function IO(n,t){return Iu(n,t)}function ix(n,t){if(!n.hasOwnProperty("model"))return!1;let e=n.model;return e.isFirstChange()?!0:!Object.is(t,e.currentValue)}function MO(n){return Object.getPrototypeOf(n.constructor)===lO}function rx(n,t){n._syncPendingControls(),t.forEach(e=>{let i=e.control;i.updateOn==="submit"&&i._pendingChange&&(e.viewToModelUpdate(i._pendingValue),i._pendingChange=!1)})}function ox(n,t){if(!t)return null;Array.isArray(t);let e,i,r;return t.forEach(o=>{o.constructor===es?e=o:MO(o)?i=o:r=o}),r||i||e||null}function TO(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}var AO={provide:di,useExisting:bt(()=>Ga)},Va=Promise.resolve(),Ga=(()=>{class n extends di{callSetDisabledState;get submitted(){return sn(this.submittedReactive)}_submitted=wn(()=>this.submittedReactive());submittedReactive=Oe(!1);_directives=new Set;form;ngSubmit=new z;options;constructor(e,i,r){super(),this.callSetDisabledState=r,this.form=new Jo({},Kg(e),Yg(i))}ngAfterViewInit(){this._setUpdateStrategy()}get formDirective(){return this}get control(){return this.form}get path(){return[]}get controls(){return this.form.controls}addControl(e){Va.then(()=>{let i=this._findContainer(e.path);e.control=i.registerControl(e.name,e.control),ku(e.control,e,this.callSetDisabledState),e.control.updateValueAndValidity({emitEvent:!1}),this._directives.add(e)})}getControl(e){return this.form.get(e.path)}removeControl(e){Va.then(()=>{this._findContainer(e.path)?.removeControl(e.name),this._directives.delete(e)})}addFormGroup(e){Va.then(()=>{let i=this._findContainer(e.path),r=new Jo({});nx(r,e),i.registerControl(e.name,r),r.updateValueAndValidity({emitEvent:!1})})}removeFormGroup(e){Va.then(()=>{this._findContainer(e.path)?.removeControl?.(e.name)})}getFormGroup(e){return this.form.get(e.path)}updateModel(e,i){Va.then(()=>{this.form.get(e.path).setValue(i)})}setValue(e){this.control.setValue(e)}onSubmit(e){return this.submittedReactive.set(!0),rx(this.form,this._directives),this.ngSubmit.emit(e),this.form._events.next(new Eu(this.control)),e?.target?.method==="dialog"}onReset(){this.resetForm()}resetForm(e=void 0){this.form.reset(e),this.submittedReactive.set(!1)}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.form._updateOn=this.options.updateOn)}_findContainer(e){return e.pop(),e.length?this.form.get(e):this.form}static \u0275fac=function(i){return new(i||n)(q(ts,10),q(Mu,10),q($a,8))};static \u0275dir=T({type:n,selectors:[["form",3,"ngNoForm","",3,"formGroup","",3,"formArray",""],["ng-form"],["","ngForm",""]],hostBindings:function(i,r){i&1&&V("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{options:[0,"ngFormOptions","options"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ve([AO]),re]})}return n})();function PD(n,t){let e=n.indexOf(t);e>-1&&n.splice(e,1)}function LD(n){return typeof n=="object"&&n!==null&&Object.keys(n).length===2&&"value"in n&&"disabled"in n}var Ha=class extends Xo{defaultValue=null;_onChange=[];_pendingValue;_pendingChange=!1;constructor(t=null,e,i){super(Zg(e),Xg(i,e)),this._applyFormState(t),this._setUpdateStrategy(e),this._initObservables(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator}),Au(e)&&(e.nonNullable||e.initialValueIsDefault)&&(LD(t)?this.defaultValue=t.value:this.defaultValue=t)}setValue(t,e={}){this.value=this._pendingValue=t,this._onChange.length&&e.emitModelToViewChange!==!1&&this._onChange.forEach(i=>i(this.value,e.emitViewToModelChange!==!1)),this.updateValueAndValidity(e)}patchValue(t,e={}){this.setValue(t,e)}reset(t=this.defaultValue,e={}){this._applyFormState(t),this.markAsPristine(e),this.markAsUntouched(e),this.setValue(this.value,e),e.overwriteDefaultValue&&(this.defaultValue=this.value),this._pendingChange=!1,e?.emitEvent!==!1&&this._events.next(new Ua(this))}_updateValue(){}_anyControls(t){return!1}_allControlsDisabled(){return this.disabled}registerOnChange(t){this._onChange.push(t)}_unregisterOnChange(t){PD(this._onChange,t)}registerOnDisabledChange(t){this._onDisabledChange.push(t)}_unregisterOnDisabledChange(t){PD(this._onDisabledChange,t)}_forEachChild(t){}_syncPendingControls(){return this.updateOn==="submit"&&(this._pendingDirty&&this.markAsDirty(),this._pendingTouched&&this.markAsTouched(),this._pendingChange)?(this.setValue(this._pendingValue,{onlySelf:!0,emitModelToViewChange:!1}),!0):!1}_applyFormState(t){LD(t)?(this.value=this._pendingValue=t.value,t.disabled?this.disable({onlySelf:!0,emitEvent:!1}):this.enable({onlySelf:!0,emitEvent:!1})):this.value=this._pendingValue=t}};var RO=n=>n instanceof Ha;var OO={provide:En,useExisting:bt(()=>e_)},BD=Promise.resolve(),e_=(()=>{class n extends En{_changeDetectorRef;callSetDisabledState;control=new Ha;static ngAcceptInputType_isDisabled;_registered=!1;viewModel;name="";isDisabled;model;options;update=new z;constructor(e,i,r,o,s,a){super(),this._changeDetectorRef=s,this.callSetDisabledState=a,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=ox(this,o)}ngOnChanges(e){if(this._checkForErrors(),!this._registered||"name"in e){if(this._registered&&(this._checkName(),this.formDirective)){let i=e.name.previousValue;this.formDirective.removeControl({name:i,path:this._getPath(i)})}this._setUpControl()}"isDisabled"in e&&this._updateDisabled(e),ix(e,this.viewModel)&&(this._updateValue(this.model),this.viewModel=this.model)}ngOnDestroy(){this.formDirective?.removeControl(this)}get path(){return this._getPath(this.name)}get formDirective(){return this._parent?this._parent.formDirective:null}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}_setUpControl(){this._setUpdateStrategy(),this._isStandalone()?this._setUpStandalone():this.formDirective.addControl(this),this._registered=!0}_setUpdateStrategy(){this.options&&this.options.updateOn!=null&&(this.control._updateOn=this.options.updateOn)}_isStandalone(){return!this._parent||!!(this.options&&this.options.standalone)}_setUpStandalone(){ku(this.control,this,this.callSetDisabledState),this.control.updateValueAndValidity({emitEvent:!1})}_checkForErrors(){this._checkName()}_checkName(){this.options&&this.options.name&&(this.name=this.options.name),!this._isStandalone()&&this.name}_updateValue(e){BD.then(()=>{this.control.setValue(e,{emitViewToModelChange:!1}),this._changeDetectorRef?.markForCheck()})}_updateDisabled(e){let i=e.isDisabled.currentValue,r=i!==0&&N(i);BD.then(()=>{r&&!this.control.disabled?this.control.disable():!r&&this.control.disabled&&this.control.enable(),this._changeDetectorRef?.markForCheck()})}_getPath(e){return this._parent?ex(e,this._parent):[e]}static \u0275fac=function(i){return new(i||n)(q(di,9),q(ts,10),q(Mu,10),q(Ur,10),q(Ae,8),q($a,8))};static \u0275dir=T({type:n,selectors:[["","ngModel","",3,"formControlName","",3,"formControl",""]],inputs:{name:"name",isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"],options:[0,"ngModelOptions","options"]},outputs:{update:"ngModelChange"},exportAs:["ngModel"],standalone:!1,features:[ve([OO]),re,Le]})}return n})();var sx=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["form",3,"ngNoForm","",3,"ngNativeValidate",""]],hostAttrs:["novalidate",""],standalone:!1})}return n})();var Wg=class extends Xo{constructor(t,e,i){super(Zg(e),Xg(i,e)),this.controls=t,this._initObservables(),this._setUpdateStrategy(e),this._setUpControls(),this.updateValueAndValidity({onlySelf:!0,emitEvent:!!this.asyncValidator})}controls;at(t){return this.controls[this._adjustIndex(t)]}push(t,e={}){Array.isArray(t)?t.forEach(i=>{this.controls.push(i),this._registerControl(i)}):(this.controls.push(t),this._registerControl(t)),this.updateValueAndValidity({emitEvent:e.emitEvent}),this._onCollectionChange()}insert(t,e,i={}){this.controls.splice(t,0,e),this._registerControl(e),this.updateValueAndValidity({emitEvent:i.emitEvent})}removeAt(t,e={}){let i=this._adjustIndex(t);i<0&&(i=0),this.controls[i]&&this.controls[i]._registerOnCollectionChange(()=>{}),this.controls.splice(i,1),this.updateValueAndValidity({emitEvent:e.emitEvent})}setControl(t,e,i={}){let r=this._adjustIndex(t);r<0&&(r=0),this.controls[r]&&this.controls[r]._registerOnCollectionChange(()=>{}),this.controls.splice(r,1),e&&(this.controls.splice(r,0,e),this._registerControl(e)),this.updateValueAndValidity({emitEvent:i.emitEvent}),this._onCollectionChange()}get length(){return this.controls.length}setValue(t,e={}){JD(this,!1,t),t.forEach((i,r)=>{XD(this,!1,r),this.at(r).setValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e)}patchValue(t,e={}){t!=null&&(t.forEach((i,r)=>{this.at(r)&&this.at(r).patchValue(i,{onlySelf:!0,emitEvent:e.emitEvent})}),this.updateValueAndValidity(e))}reset(t=[],e={}){this._forEachChild((i,r)=>{i.reset(t[r],be(M({},e),{onlySelf:!0}))}),this._updatePristine(e,this),this._updateTouched(e,this),this.updateValueAndValidity(e),e?.emitEvent!==!1&&this._events.next(new Ua(this))}getRawValue(){return this.controls.map(t=>t.getRawValue())}clear(t={}){this.controls.length<1||(this._forEachChild(e=>e._registerOnCollectionChange(()=>{})),this.controls.splice(0),this.updateValueAndValidity({emitEvent:t.emitEvent}))}_adjustIndex(t){return t<0?t+this.length:t}_syncPendingControls(){let t=this.controls.reduce((e,i)=>i._syncPendingControls()?!0:e,!1);return t&&this.updateValueAndValidity({onlySelf:!0}),t}_forEachChild(t){this.controls.forEach((e,i)=>{t(e,i)})}_updateValue(){this.value=this.controls.filter(t=>t.enabled||this.disabled).map(t=>t.value)}_anyControls(t){return this.controls.some(e=>e.enabled&&t(e))}_setUpControls(){this._forEachChild(t=>this._registerControl(t))}_allControlsDisabled(){for(let t of this.controls)if(t.enabled)return!1;return this.controls.length>0||this.disabled}_registerControl(t){t.setParent(this),t._registerOnCollectionChange(this._onCollectionChange)}_find(t){return this.at(t)??null}};var NO=(()=>{class n extends di{callSetDisabledState;get submitted(){return sn(this._submittedReactive)}set submitted(e){this._submittedReactive.set(e)}_submitted=wn(()=>this._submittedReactive());_submittedReactive=Oe(!1);_oldForm;_onCollectionChange=()=>this._updateDomValue();directives=[];constructor(e,i,r){super(),this.callSetDisabledState=r,this._setValidators(e),this._setAsyncValidators(i)}ngOnChanges(e){this.onChanges(e)}ngOnDestroy(){this.onDestroy()}onChanges(e){this._checkFormPresent(),e.hasOwnProperty("form")&&(this._updateValidators(),this._updateDomValue(),this._updateRegistrations(),this._oldForm=this.form)}onDestroy(){this.form&&(Iu(this.form,this),this.form._onCollectionChange===this._onCollectionChange&&this.form._registerOnCollectionChange(()=>{}))}get formDirective(){return this}get path(){return[]}addControl(e){let i=this.form.get(e.path);return ku(i,e,this.callSetDisabledState),i.updateValueAndValidity({emitEvent:!1}),this.directives.push(e),i}getControl(e){return this.form.get(e.path)}removeControl(e){FD(e.control||null,e,!1),TO(this.directives,e)}addFormGroup(e){this._setUpFormContainer(e)}removeFormGroup(e){this._cleanUpFormContainer(e)}getFormGroup(e){return this.form.get(e.path)}getFormArray(e){return this.form.get(e.path)}addFormArray(e){this._setUpFormContainer(e)}removeFormArray(e){this._cleanUpFormContainer(e)}updateModel(e,i){this.form.get(e.path).setValue(i)}onReset(){this.resetForm()}resetForm(e=void 0,i={}){this.form.reset(e,i),this._submittedReactive.set(!1)}onSubmit(e){return this.submitted=!0,rx(this.form,this.directives),this.ngSubmit.emit(e),this.form._events.next(new Eu(this.control)),e?.target?.method==="dialog"}_updateDomValue(){this.directives.forEach(e=>{let i=e.control,r=this.form.get(e.path);i!==r&&(FD(i||null,e),RO(r)&&(ku(r,e,this.callSetDisabledState),e.control=r))}),this.form._updateTreeValidity({emitEvent:!1})}_setUpFormContainer(e){let i=this.form.get(e.path);nx(i,e),i.updateValueAndValidity({emitEvent:!1})}_cleanUpFormContainer(e){let i=this.form?.get(e.path);i&&IO(i,e)&&i.updateValueAndValidity({emitEvent:!1})}_updateRegistrations(){this.form._registerOnCollectionChange(this._onCollectionChange),this._oldForm?._registerOnCollectionChange(()=>{})}_updateValidators(){Jg(this.form,this),this._oldForm&&Iu(this._oldForm,this)}_checkFormPresent(){this.form}static \u0275fac=function(i){return new(i||n)(q(ts,10),q(Mu,10),q($a,8))};static \u0275dir=T({type:n,features:[re,Le]})}return n})();var ax=new v("");var FO={provide:En,useExisting:bt(()=>t_)},t_=(()=>{class n extends En{_ngModelWarningConfig;_added=!1;viewModel;control;name=null;set isDisabled(e){}model;update=new z;static _ngModelWarningSentOnce=!1;_ngModelWarningSent=!1;constructor(e,i,r,o,s){super(),this._ngModelWarningConfig=s,this._parent=e,this._setValidators(i),this._setAsyncValidators(r),this.valueAccessor=ox(this,o)}ngOnChanges(e){this._added||this._setUpControl(),ix(e,this.viewModel)&&(this.viewModel=this.model,this.formDirective.updateModel(this,this.model))}ngOnDestroy(){this.formDirective?.removeControl(this)}viewToModelUpdate(e){this.viewModel=e,this.update.emit(e)}get path(){return ex(this.name==null?this.name:this.name.toString(),this._parent)}get formDirective(){return this._parent?this._parent.formDirective:null}_setUpControl(){this.control=this.formDirective.addControl(this),this._added=!0}static \u0275fac=function(i){return new(i||n)(q(di,13),q(ts,10),q(Mu,10),q(Ur,10),q(ax,8))};static \u0275dir=T({type:n,selectors:[["","formControlName",""]],inputs:{name:[0,"formControlName","name"],isDisabled:[0,"disabled","isDisabled"],model:[0,"ngModel","model"]},outputs:{update:"ngModelChange"},standalone:!1,features:[ve([FO]),re,Le]})}return n})();var PO={provide:di,useExisting:bt(()=>$r)},$r=(()=>{class n extends NO{form=null;ngSubmit=new z;get control(){return this.form}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["","formGroup",""]],hostBindings:function(i,r){i&1&&V("submit",function(s){return r.onSubmit(s)})("reset",function(){return r.onReset()})},inputs:{form:[0,"formGroup","form"]},outputs:{ngSubmit:"ngSubmit"},exportAs:["ngForm"],standalone:!1,features:[ve([PO]),re]})}return n})();var lx=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})();function VD(n){return!!n&&(n.asyncValidators!==void 0||n.validators!==void 0||n.updateOn!==void 0)}var cx=(()=>{class n{useNonNullable=!1;get nonNullable(){let e=new n;return e.useNonNullable=!0,e}group(e,i=null){let r=this._reduceControls(e),o={};return VD(i)?o=i:i!==null&&(o.validators=i.validator,o.asyncValidators=i.asyncValidator),new Jo(r,o)}record(e,i=null){let r=this._reduceControls(e);return new Gg(r,i)}control(e,i,r){let o={};return this.useNonNullable?(VD(i)?o=i:(o.validators=i,o.asyncValidators=r),new Ha(e,be(M({},o),{nonNullable:!0}))):new Ha(e,i,r)}array(e,i,r){let o=e.map(s=>this._createControl(s));return new Wg(o,i,r)}_reduceControls(e){let i={};return Object.keys(e).forEach(r=>{i[r]=this._createControl(e[r])}),i}_createControl(e){if(e instanceof Ha)return e;if(e instanceof Xo)return e;if(Array.isArray(e)){let i=e[0],r=e.length>1?e[1]:null,o=e.length>2?e[2]:null;return this.control(i,r,o)}else return this.control(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var dx=(()=>{class n{static withConfig(e){return{ngModule:n,providers:[{provide:$a,useValue:e.callSetDisabledState??Ru}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[lx]})}return n})(),ux=(()=>{class n{static withConfig(e){return{ngModule:n,providers:[{provide:ax,useValue:e.warnOnNgModelWithFormControl??"always"},{provide:$a,useValue:e.callSetDisabledState??Ru}]}}static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[lx]})}return n})();var Wa=class{};var Gi=(()=>{class n{constructor(){this.questionsReplaySubject=new Kt(1)}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}}return n})();function qa(n){return n.buttons===0||n.detail===0}function Qa(n){let t=n.touches&&n.touches[0]||n.changedTouches&&n.changedTouches[0];return!!t&&t.identifier===-1&&(t.radiusX==null||t.radiusX===1)&&(t.radiusY==null||t.radiusY===1)}var n_;function hx(){if(n_==null){let n=typeof document<"u"?document.head:null;n_=!!(n&&(n.createShadowRoot||n.attachShadow))}return n_}function i_(n){if(hx()){let t=n.getRootNode?n.getRootNode():null;if(typeof ShadowRoot<"u"&&ShadowRoot&&t instanceof ShadowRoot)return t}return null}function Ka(){let n=typeof document<"u"&&document?document.activeElement:null;for(;n&&n.shadowRoot;){let t=n.shadowRoot.activeElement;if(t===n)break;n=t}return n}function ft(n){return n.composedPath?n.composedPath()[0]:n.target}var r_;try{r_=typeof Intl<"u"&&Intl.v8BreakIterator}catch(n){r_=!1}var _e=(()=>{class n{_platformId=u(Nr);isBrowser=this._platformId?aC(this._platformId):typeof document=="object"&&!!document;EDGE=this.isBrowser&&/(edge)/i.test(navigator.userAgent);TRIDENT=this.isBrowser&&/(msie|trident)/i.test(navigator.userAgent);BLINK=this.isBrowser&&!!(window.chrome||r_)&&typeof CSS<"u"&&!this.EDGE&&!this.TRIDENT;WEBKIT=this.isBrowser&&/AppleWebKit/i.test(navigator.userAgent)&&!this.BLINK&&!this.EDGE&&!this.TRIDENT;IOS=this.isBrowser&&/iPad|iPhone|iPod/.test(navigator.userAgent)&&!("MSStream"in window);FIREFOX=this.isBrowser&&/(firefox|minefield)/i.test(navigator.userAgent);ANDROID=this.isBrowser&&/android/i.test(navigator.userAgent)&&!this.TRIDENT;SAFARI=this.isBrowser&&/safari/i.test(navigator.userAgent)&&this.WEBKIT;constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Ya;function fx(){if(Ya==null&&typeof window<"u")try{window.addEventListener("test",null,Object.defineProperty({},"passive",{get:()=>Ya=!0}))}finally{Ya=Ya||!1}return Ya}function ns(n){return fx()?n:!!n.capture}function ui(n,t=0){return Nu(n)?Number(n):arguments.length===2?t:0}function Nu(n){return!isNaN(parseFloat(n))&&!isNaN(Number(n))}function Wt(n){return n instanceof P?n.nativeElement:n}var px=new v("cdk-input-modality-detector-options"),gx={ignoreKeys:[18,17,224,91,16]},_x=650,o_={passive:!0,capture:!0},vx=(()=>{class n{_platform=u(_e);_listenerCleanups;modalityDetected;modalityChanged;get mostRecentModality(){return this._modality.value}_mostRecentTarget=null;_modality=new Bt(null);_options;_lastTouchMs=0;_onKeydown=e=>{this._options?.ignoreKeys?.some(i=>i===e.keyCode)||(this._modality.next("keyboard"),this._mostRecentTarget=ft(e))};_onMousedown=e=>{Date.now()-this._lastTouchMs<_x||(this._modality.next(qa(e)?"keyboard":"mouse"),this._mostRecentTarget=ft(e))};_onTouchstart=e=>{if(Qa(e)){this._modality.next("keyboard");return}this._lastTouchMs=Date.now(),this._modality.next("touch"),this._mostRecentTarget=ft(e)};constructor(){let e=u(F),i=u(U),r=u(px,{optional:!0});if(this._options=M(M({},gx),r),this.modalityDetected=this._modality.pipe(As(1)),this.modalityChanged=this.modalityDetected.pipe(hc()),this._platform.isBrowser){let o=u(qe).createRenderer(null,null);this._listenerCleanups=e.runOutsideAngular(()=>[o.listen(i,"keydown",this._onKeydown,o_),o.listen(i,"mousedown",this._onMousedown,o_),o.listen(i,"touchstart",this._onTouchstart,o_)])}}ngOnDestroy(){this._modality.complete(),this._listenerCleanups?.forEach(e=>e())}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),Za=(function(n){return n[n.IMMEDIATE=0]="IMMEDIATE",n[n.EVENTUAL=1]="EVENTUAL",n})(Za||{}),bx=new v("cdk-focus-monitor-default-options"),Fu=ns({passive:!0,capture:!0}),mi=(()=>{class n{_ngZone=u(F);_platform=u(_e);_inputModalityDetector=u(vx);_origin=null;_lastFocusOrigin=null;_windowFocused=!1;_windowFocusTimeoutId;_originTimeoutId;_originFromTouchInteraction=!1;_elementInfo=new Map;_monitoredElementCount=0;_rootNodeFocusListenerCount=new Map;_detectionMode;_windowFocusListener=()=>{this._windowFocused=!0,this._windowFocusTimeoutId=setTimeout(()=>this._windowFocused=!1)};_document=u(U);_stopInputModalityDetector=new k;constructor(){let e=u(bx,{optional:!0});this._detectionMode=e?.detectionMode||Za.IMMEDIATE}_rootNodeFocusAndBlurListener=e=>{let i=ft(e);for(let r=i;r;r=r.parentElement)e.type==="focus"?this._onFocus(e,r):this._onBlur(e,r)};monitor(e,i=!1){let r=Wt(e);if(!this._platform.isBrowser||r.nodeType!==1)return $e();let o=i_(r)||this._document,s=this._elementInfo.get(r);if(s)return i&&(s.checkChildren=!0),s.subject;let a={checkChildren:i,subject:new k,rootNode:o};return this._elementInfo.set(r,a),this._registerGlobalListeners(a),a.subject}stopMonitoring(e){let i=Wt(e),r=this._elementInfo.get(i);r&&(r.subject.complete(),this._setClasses(i),this._elementInfo.delete(i),this._removeGlobalListeners(r))}focusVia(e,i,r){let o=Wt(e),s=this._document.activeElement;o===s?this._getClosestElementsInfo(o).forEach(([a,l])=>this._originChanged(a,i,l)):(this._setOrigin(i),typeof o.focus=="function"&&o.focus(r))}ngOnDestroy(){this._elementInfo.forEach((e,i)=>this.stopMonitoring(i))}_getWindow(){return this._document.defaultView||window}_getFocusOrigin(e){return this._origin?this._originFromTouchInteraction?this._shouldBeAttributedToTouch(e)?"touch":"program":this._origin:this._windowFocused&&this._lastFocusOrigin?this._lastFocusOrigin:e&&this._isLastInteractionFromInputLabel(e)?"mouse":"program"}_shouldBeAttributedToTouch(e){return this._detectionMode===Za.EVENTUAL||!!e?.contains(this._inputModalityDetector._mostRecentTarget)}_setClasses(e,i){e.classList.toggle("cdk-focused",!!i),e.classList.toggle("cdk-touch-focused",i==="touch"),e.classList.toggle("cdk-keyboard-focused",i==="keyboard"),e.classList.toggle("cdk-mouse-focused",i==="mouse"),e.classList.toggle("cdk-program-focused",i==="program")}_setOrigin(e,i=!1){this._ngZone.runOutsideAngular(()=>{if(this._origin=e,this._originFromTouchInteraction=e==="touch"&&i,this._detectionMode===Za.IMMEDIATE){clearTimeout(this._originTimeoutId);let r=this._originFromTouchInteraction?_x:1;this._originTimeoutId=setTimeout(()=>this._origin=null,r)}})}_onFocus(e,i){let r=this._elementInfo.get(i),o=ft(e);!r||!r.checkChildren&&i!==o||this._originChanged(i,this._getFocusOrigin(o),r)}_onBlur(e,i){let r=this._elementInfo.get(i);!r||r.checkChildren&&e.relatedTarget instanceof Node&&i.contains(e.relatedTarget)||(this._setClasses(i),this._emitOrigin(r,null))}_emitOrigin(e,i){e.subject.observers.length&&this._ngZone.run(()=>e.subject.next(i))}_registerGlobalListeners(e){if(!this._platform.isBrowser)return;let i=e.rootNode,r=this._rootNodeFocusListenerCount.get(i)||0;r||this._ngZone.runOutsideAngular(()=>{i.addEventListener("focus",this._rootNodeFocusAndBlurListener,Fu),i.addEventListener("blur",this._rootNodeFocusAndBlurListener,Fu)}),this._rootNodeFocusListenerCount.set(i,r+1),++this._monitoredElementCount===1&&(this._ngZone.runOutsideAngular(()=>{this._getWindow().addEventListener("focus",this._windowFocusListener)}),this._inputModalityDetector.modalityDetected.pipe(Ce(this._stopInputModalityDetector)).subscribe(o=>{this._setOrigin(o,!0)}))}_removeGlobalListeners(e){let i=e.rootNode;if(this._rootNodeFocusListenerCount.has(i)){let r=this._rootNodeFocusListenerCount.get(i);r>1?this._rootNodeFocusListenerCount.set(i,r-1):(i.removeEventListener("focus",this._rootNodeFocusAndBlurListener,Fu),i.removeEventListener("blur",this._rootNodeFocusAndBlurListener,Fu),this._rootNodeFocusListenerCount.delete(i))}--this._monitoredElementCount||(this._getWindow().removeEventListener("focus",this._windowFocusListener),this._stopInputModalityDetector.next(),clearTimeout(this._windowFocusTimeoutId),clearTimeout(this._originTimeoutId))}_originChanged(e,i,r){this._setClasses(e,i),this._emitOrigin(r,i),this._lastFocusOrigin=i}_getClosestElementsInfo(e){let i=[];return this._elementInfo.forEach((r,o)=>{(o===e||r.checkChildren&&o.contains(e))&&i.push([o,r])}),i}_isLastInteractionFromInputLabel(e){let{_mostRecentTarget:i,mostRecentModality:r}=this._inputModalityDetector;if(r!=="mouse"||!i||i===e||e.nodeName!=="INPUT"&&e.nodeName!=="TEXTAREA"||e.disabled)return!1;let o=e.labels;if(o){for(let s=0;s<o.length;s++)if(o[s].contains(i))return!0}return!1}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Pu=new WeakMap,it=(()=>{class n{_appRef;_injector=u($);_environmentInjector=u(Ze);load(e){let i=this._appRef=this._appRef||this._injector.get(Ut),r=Pu.get(i);r||(r={loaders:new Set,refs:[]},Pu.set(i,r),i.onDestroy(()=>{Pu.get(i)?.refs.forEach(o=>o.destroy()),Pu.delete(i)})),r.loaders.has(e)||(r.loaders.add(e),r.refs.push(Bd(e,{environmentInjector:this._environmentInjector})))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var is=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["ng-component"]],exportAs:["cdkVisuallyHidden"],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-visually-hidden {
  border: 0;
  clip: rect(0 0 0 0);
  height: 1px;
  margin: -1px;
  overflow: hidden;
  padding: 0;
  position: absolute;
  width: 1px;
  white-space: nowrap;
  outline: 0;
  -webkit-appearance: none;
  -moz-appearance: none;
  left: 0;
}
[dir=rtl] .cdk-visually-hidden {
  left: auto;
  right: 0;
}
`],encapsulation:2,changeDetection:0})}return n})(),Lu;function LO(){if(Lu===void 0&&(Lu=null,typeof window<"u")){let n=window;n.trustedTypes!==void 0&&(Lu=n.trustedTypes.createPolicy("angular#components",{createHTML:t=>t}))}return Lu}function Gr(n){return LO()?.createHTML(n)||n}function yx(n,t,e){let i=e.sanitize(ct.HTML,t);n.innerHTML=Gr(i||"")}function rs(n){return Array.isArray(n)?n:[n]}var wx=new Set,Wr,os=(()=>{class n{_platform=u(_e);_nonce=u(Fr,{optional:!0});_matchMedia;constructor(){this._matchMedia=this._platform.isBrowser&&window.matchMedia?window.matchMedia.bind(window):VO}matchMedia(e){return(this._platform.WEBKIT||this._platform.BLINK)&&BO(e,this._nonce),this._matchMedia(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function BO(n,t){if(!wx.has(n))try{Wr||(Wr=document.createElement("style"),t&&Wr.setAttribute("nonce",t),Wr.setAttribute("type","text/css"),document.head.appendChild(Wr)),Wr.sheet&&(Wr.sheet.insertRule(`@media ${n} {body{ }}`,0),wx.add(n))}catch(e){console.error(e)}}function VO(n){return{matches:n==="all"||n==="",media:n,addListener:()=>{},removeListener:()=>{}}}var hi=(()=>{class n{_mediaMatcher=u(os);_zone=u(F);_queries=new Map;_destroySubject=new k;constructor(){}ngOnDestroy(){this._destroySubject.next(),this._destroySubject.complete()}isMatched(e){return Cx(rs(e)).some(r=>this._registerQuery(r).mql.matches)}observe(e){let r=Cx(rs(e)).map(s=>this._registerQuery(s).observable),o=hn(r);return o=mo(o.pipe(Vt(1)),o.pipe(As(1),Ms(0))),o.pipe(ye(s=>{let a={matches:!1,breakpoints:{}};return s.forEach(({matches:l,query:c})=>{a.matches=a.matches||l,a.breakpoints[c]=l}),a}))}_registerQuery(e){if(this._queries.has(e))return this._queries.get(e);let i=this._mediaMatcher.matchMedia(e),o={observable:new ae(s=>{let a=l=>this._zone.run(()=>s.next(l));return i.addListener(a),()=>{i.removeListener(a)}}).pipe(jt(i),ye(({matches:s})=>({query:e,matches:s})),Ce(this._destroySubject)),mql:i};return this._queries.set(e,o),o}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Cx(n){return n.map(t=>t.split(",")).reduce((t,e)=>t.concat(e)).map(t=>t.trim())}var jO=(()=>{class n{create(e){return typeof MutationObserver>"u"?null:new MutationObserver(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var ss=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[jO]})}return n})();var Bu=(()=>{class n{_platform=u(_e);constructor(){}isDisabled(e){return e.hasAttribute("disabled")}isVisible(e){return HO(e)&&getComputedStyle(e).visibility==="visible"}isTabbable(e){if(!this._platform.isBrowser)return!1;let i=zO(YO(e));if(i&&(Dx(i)===-1||!this.isVisible(i)))return!1;let r=e.nodeName.toLowerCase(),o=Dx(e);return e.hasAttribute("contenteditable")?o!==-1:r==="iframe"||r==="object"||this._platform.WEBKIT&&this._platform.IOS&&!QO(e)?!1:r==="audio"?e.hasAttribute("controls")?o!==-1:!1:r==="video"?o===-1?!1:o!==null?!0:this._platform.FIREFOX||e.hasAttribute("controls"):e.tabIndex>=0}isFocusable(e,i){return KO(e)&&!this.isDisabled(e)&&(i?.ignoreVisibility||this.isVisible(e))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function zO(n){try{return n.frameElement}catch(t){return null}}function HO(n){return!!(n.offsetWidth||n.offsetHeight||typeof n.getClientRects=="function"&&n.getClientRects().length)}function UO(n){let t=n.nodeName.toLowerCase();return t==="input"||t==="select"||t==="button"||t==="textarea"}function $O(n){return WO(n)&&n.type=="hidden"}function GO(n){return qO(n)&&n.hasAttribute("href")}function WO(n){return n.nodeName.toLowerCase()=="input"}function qO(n){return n.nodeName.toLowerCase()=="a"}function kx(n){if(!n.hasAttribute("tabindex")||n.tabIndex===void 0)return!1;let t=n.getAttribute("tabindex");return!!(t&&!isNaN(parseInt(t,10)))}function Dx(n){if(!kx(n))return null;let t=parseInt(n.getAttribute("tabindex")||"",10);return isNaN(t)?-1:t}function QO(n){let t=n.nodeName.toLowerCase(),e=t==="input"&&n.type;return e==="text"||e==="password"||t==="select"||t==="textarea"}function KO(n){return $O(n)?!1:UO(n)||GO(n)||n.hasAttribute("contenteditable")||kx(n)}function YO(n){return n.ownerDocument&&n.ownerDocument.defaultView||window}var a_=class{_element;_checker;_ngZone;_document;_injector;_startAnchor=null;_endAnchor=null;_hasAttached=!1;startAnchorListener=()=>this.focusLastTabbableElement();endAnchorListener=()=>this.focusFirstTabbableElement();get enabled(){return this._enabled}set enabled(t){this._enabled=t,this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(t,this._startAnchor),this._toggleAnchorTabIndex(t,this._endAnchor))}_enabled=!0;constructor(t,e,i,r,o=!1,s){this._element=t,this._checker=e,this._ngZone=i,this._document=r,this._injector=s,o||this.attachAnchors()}destroy(){let t=this._startAnchor,e=this._endAnchor;t&&(t.removeEventListener("focus",this.startAnchorListener),t.remove()),e&&(e.removeEventListener("focus",this.endAnchorListener),e.remove()),this._startAnchor=this._endAnchor=null,this._hasAttached=!1}attachAnchors(){return this._hasAttached?!0:(this._ngZone.runOutsideAngular(()=>{this._startAnchor||(this._startAnchor=this._createAnchor(),this._startAnchor.addEventListener("focus",this.startAnchorListener)),this._endAnchor||(this._endAnchor=this._createAnchor(),this._endAnchor.addEventListener("focus",this.endAnchorListener))}),this._element.parentNode&&(this._element.parentNode.insertBefore(this._startAnchor,this._element),this._element.parentNode.insertBefore(this._endAnchor,this._element.nextSibling),this._hasAttached=!0),this._hasAttached)}focusInitialElementWhenReady(t){return new Promise(e=>{this._executeOnStable(()=>e(this.focusInitialElement(t)))})}focusFirstTabbableElementWhenReady(t){return new Promise(e=>{this._executeOnStable(()=>e(this.focusFirstTabbableElement(t)))})}focusLastTabbableElementWhenReady(t){return new Promise(e=>{this._executeOnStable(()=>e(this.focusLastTabbableElement(t)))})}_getRegionBoundary(t){let e=this._element.querySelectorAll(`[cdk-focus-region-${t}], [cdkFocusRegion${t}], [cdk-focus-${t}]`);return t=="start"?e.length?e[0]:this._getFirstTabbableElement(this._element):e.length?e[e.length-1]:this._getLastTabbableElement(this._element)}focusInitialElement(t){let e=this._element.querySelector("[cdk-focus-initial], [cdkFocusInitial]");if(e){if(!this._checker.isFocusable(e)){let i=this._getFirstTabbableElement(e);return i?.focus(t),!!i}return e.focus(t),!0}return this.focusFirstTabbableElement(t)}focusFirstTabbableElement(t){let e=this._getRegionBoundary("start");return e&&e.focus(t),!!e}focusLastTabbableElement(t){let e=this._getRegionBoundary("end");return e&&e.focus(t),!!e}hasAttached(){return this._hasAttached}_getFirstTabbableElement(t){if(this._checker.isFocusable(t)&&this._checker.isTabbable(t))return t;let e=t.children;for(let i=0;i<e.length;i++){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getFirstTabbableElement(e[i]):null;if(r)return r}return null}_getLastTabbableElement(t){if(this._checker.isFocusable(t)&&this._checker.isTabbable(t))return t;let e=t.children;for(let i=e.length-1;i>=0;i--){let r=e[i].nodeType===this._document.ELEMENT_NODE?this._getLastTabbableElement(e[i]):null;if(r)return r}return null}_createAnchor(){let t=this._document.createElement("div");return this._toggleAnchorTabIndex(this._enabled,t),t.classList.add("cdk-visually-hidden"),t.classList.add("cdk-focus-trap-anchor"),t.setAttribute("aria-hidden","true"),t}_toggleAnchorTabIndex(t,e){t?e.setAttribute("tabindex","0"):e.removeAttribute("tabindex")}toggleAnchors(t){this._startAnchor&&this._endAnchor&&(this._toggleAnchorTabIndex(t,this._startAnchor),this._toggleAnchorTabIndex(t,this._endAnchor))}_executeOnStable(t){this._injector?Qe(t,{injector:this._injector}):setTimeout(t)}},l_=(()=>{class n{_checker=u(Bu);_ngZone=u(F);_document=u(U);_injector=u($);constructor(){u(it).load(is)}create(e,i=!1){return new a_(e,this._checker,this._ngZone,this._document,i,this._injector)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Sx=new v("liveAnnouncerElement",{providedIn:"root",factory:()=>null}),Ix=new v("LIVE_ANNOUNCER_DEFAULT_OPTIONS"),ZO=0,Xa=(()=>{class n{_ngZone=u(F);_defaultOptions=u(Ix,{optional:!0});_liveElement;_document=u(U);_sanitizer=u(Ia);_previousTimeout;_currentPromise;_currentResolve;constructor(){let e=u(Sx,{optional:!0});this._liveElement=e||this._createLiveElement()}announce(e,...i){let r=this._defaultOptions,o,s;return i.length===1&&typeof i[0]=="number"?s=i[0]:[o,s]=i,this.clear(),clearTimeout(this._previousTimeout),o||(o=r&&r.politeness?r.politeness:"polite"),s==null&&r&&(s=r.duration),this._liveElement.setAttribute("aria-live",o),this._liveElement.id&&this._exposeAnnouncerToModals(this._liveElement.id),this._ngZone.runOutsideAngular(()=>(this._currentPromise||(this._currentPromise=new Promise(a=>this._currentResolve=a)),clearTimeout(this._previousTimeout),this._previousTimeout=setTimeout(()=>{!e||typeof e=="string"?this._liveElement.textContent=e:yx(this._liveElement,e,this._sanitizer),typeof s=="number"&&(this._previousTimeout=setTimeout(()=>this.clear(),s)),this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0},100),this._currentPromise))}clear(){this._liveElement&&(this._liveElement.textContent="")}ngOnDestroy(){clearTimeout(this._previousTimeout),this._liveElement?.remove(),this._liveElement=null,this._currentResolve?.(),this._currentPromise=this._currentResolve=void 0}_createLiveElement(){let e="cdk-live-announcer-element",i=this._document.getElementsByClassName(e),r=this._document.createElement("div");for(let o=0;o<i.length;o++)i[o].remove();return r.classList.add(e),r.classList.add("cdk-visually-hidden"),r.setAttribute("aria-atomic","true"),r.setAttribute("aria-live","polite"),r.id=`cdk-live-announcer-${ZO++}`,this._document.body.appendChild(r),r}_exposeAnnouncerToModals(e){let i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var Wi=(function(n){return n[n.NONE=0]="NONE",n[n.BLACK_ON_WHITE=1]="BLACK_ON_WHITE",n[n.WHITE_ON_BLACK=2]="WHITE_ON_BLACK",n})(Wi||{}),xx="cdk-high-contrast-black-on-white",Ex="cdk-high-contrast-white-on-black",s_="cdk-high-contrast-active",Mx=(()=>{class n{_platform=u(_e);_hasCheckedHighContrastMode=!1;_document=u(U);_breakpointSubscription;constructor(){this._breakpointSubscription=u(hi).observe("(forced-colors: active)").subscribe(()=>{this._hasCheckedHighContrastMode&&(this._hasCheckedHighContrastMode=!1,this._applyBodyHighContrastModeCssClasses())})}getHighContrastMode(){if(!this._platform.isBrowser)return Wi.NONE;let e=this._document.createElement("div");e.style.backgroundColor="rgb(1,2,3)",e.style.position="absolute",this._document.body.appendChild(e);let i=this._document.defaultView||window,r=i&&i.getComputedStyle?i.getComputedStyle(e):null,o=(r&&r.backgroundColor||"").replace(/ /g,"");switch(e.remove(),o){case"rgb(0,0,0)":case"rgb(45,50,54)":case"rgb(32,32,32)":return Wi.WHITE_ON_BLACK;case"rgb(255,255,255)":case"rgb(255,250,239)":return Wi.BLACK_ON_WHITE}return Wi.NONE}ngOnDestroy(){this._breakpointSubscription.unsubscribe()}_applyBodyHighContrastModeCssClasses(){if(!this._hasCheckedHighContrastMode&&this._platform.isBrowser&&this._document.body){let e=this._document.body.classList;e.remove(s_,xx,Ex),this._hasCheckedHighContrastMode=!0;let i=this.getHighContrastMode();i===Wi.BLACK_ON_WHITE?e.add(s_,xx):i===Wi.WHITE_ON_BLACK&&e.add(s_,Ex)}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),qr=(()=>{class n{constructor(){u(Mx)._applyBodyHighContrastModeCssClasses()}static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[ss]})}return n})();var XO=200,Vu=class{_letterKeyStream=new k;_items=[];_selectedItemIndex=-1;_pressedLetters=[];_skipPredicateFn;_selectedItem=new k;selectedItem=this._selectedItem;constructor(t,e){let i=typeof e?.debounceInterval=="number"?e.debounceInterval:XO;e?.skipPredicate&&(this._skipPredicateFn=e.skipPredicate),this.setItems(t),this._setupKeyHandler(i)}destroy(){this._pressedLetters=[],this._letterKeyStream.complete(),this._selectedItem.complete()}setCurrentSelectedItemIndex(t){this._selectedItemIndex=t}setItems(t){this._items=t}handleKey(t){let e=t.keyCode;t.key&&t.key.length===1?this._letterKeyStream.next(t.key.toLocaleUpperCase()):(e>=65&&e<=90||e>=48&&e<=57)&&this._letterKeyStream.next(String.fromCharCode(e))}isTyping(){return this._pressedLetters.length>0}reset(){this._pressedLetters=[]}_setupKeyHandler(t){this._letterKeyStream.pipe(pr(e=>this._pressedLetters.push(e)),Ms(t),ze(()=>this._pressedLetters.length>0),ye(()=>this._pressedLetters.join("").toLocaleUpperCase())).subscribe(e=>{for(let i=1;i<this._items.length+1;i++){let r=(this._selectedItemIndex+i)%this._items.length,o=this._items[r];if(!this._skipPredicateFn?.(o)&&o.getLabel?.().toLocaleUpperCase().trim().indexOf(e)===0){this._selectedItem.next(o);break}}this._pressedLetters=[]})}};function ut(n,...t){return t.length?t.some(e=>n[e]):n.altKey||n.shiftKey||n.ctrlKey||n.metaKey}var ju=class{_items;_activeItemIndex=Oe(-1);_activeItem=Oe(null);_wrap=!1;_typeaheadSubscription=ge.EMPTY;_itemChangesSubscription;_vertical=!0;_horizontal=null;_allowedModifierKeys=[];_homeAndEnd=!1;_pageUpAndDown={enabled:!1,delta:10};_effectRef;_typeahead;_skipPredicateFn=t=>t.disabled;constructor(t,e){this._items=t,t instanceof Pi?this._itemChangesSubscription=t.changes.subscribe(i=>this._itemsChanged(i.toArray())):Hn(t)&&(this._effectRef=Fi(()=>this._itemsChanged(t()),{injector:e}))}tabOut=new k;change=new k;skipPredicate(t){return this._skipPredicateFn=t,this}withWrap(t=!0){return this._wrap=t,this}withVerticalOrientation(t=!0){return this._vertical=t,this}withHorizontalOrientation(t){return this._horizontal=t,this}withAllowedModifierKeys(t){return this._allowedModifierKeys=t,this}withTypeAhead(t=200){this._typeaheadSubscription.unsubscribe();let e=this._getItemsArray();return this._typeahead=new Vu(e,{debounceInterval:typeof t=="number"?t:void 0,skipPredicate:i=>this._skipPredicateFn(i)}),this._typeaheadSubscription=this._typeahead.selectedItem.subscribe(i=>{this.setActiveItem(i)}),this}cancelTypeahead(){return this._typeahead?.reset(),this}withHomeAndEnd(t=!0){return this._homeAndEnd=t,this}withPageUpDown(t=!0,e=10){return this._pageUpAndDown={enabled:t,delta:e},this}setActiveItem(t){let e=this._activeItem();this.updateActiveItem(t),this._activeItem()!==e&&this.change.next(this._activeItemIndex())}onKeydown(t){let e=t.keyCode,r=["altKey","ctrlKey","metaKey","shiftKey"].every(o=>!t[o]||this._allowedModifierKeys.indexOf(o)>-1);switch(e){case 9:this.tabOut.next();return;case 40:if(this._vertical&&r){this.setNextItemActive();break}else return;case 38:if(this._vertical&&r){this.setPreviousItemActive();break}else return;case 39:if(this._horizontal&&r){this._horizontal==="rtl"?this.setPreviousItemActive():this.setNextItemActive();break}else return;case 37:if(this._horizontal&&r){this._horizontal==="rtl"?this.setNextItemActive():this.setPreviousItemActive();break}else return;case 36:if(this._homeAndEnd&&r){this.setFirstItemActive();break}else return;case 35:if(this._homeAndEnd&&r){this.setLastItemActive();break}else return;case 33:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()-this._pageUpAndDown.delta;this._setActiveItemByIndex(o>0?o:0,1);break}else return;case 34:if(this._pageUpAndDown.enabled&&r){let o=this._activeItemIndex()+this._pageUpAndDown.delta,s=this._getItemsArray().length;this._setActiveItemByIndex(o<s?o:s-1,-1);break}else return;default:(r||ut(t,"shiftKey"))&&this._typeahead?.handleKey(t);return}this._typeahead?.reset(),t.preventDefault()}get activeItemIndex(){return this._activeItemIndex()}get activeItem(){return this._activeItem()}isTyping(){return!!this._typeahead&&this._typeahead.isTyping()}setFirstItemActive(){this._setActiveItemByIndex(0,1)}setLastItemActive(){this._setActiveItemByIndex(this._getItemsArray().length-1,-1)}setNextItemActive(){this._activeItemIndex()<0?this.setFirstItemActive():this._setActiveItemByDelta(1)}setPreviousItemActive(){this._activeItemIndex()<0&&this._wrap?this.setLastItemActive():this._setActiveItemByDelta(-1)}updateActiveItem(t){let e=this._getItemsArray(),i=typeof t=="number"?t:e.indexOf(t),r=e[i];this._activeItem.set(r??null),this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i)}destroy(){this._typeaheadSubscription.unsubscribe(),this._itemChangesSubscription?.unsubscribe(),this._effectRef?.destroy(),this._typeahead?.destroy(),this.tabOut.complete(),this.change.complete()}_setActiveItemByDelta(t){this._wrap?this._setActiveInWrapMode(t):this._setActiveInDefaultMode(t)}_setActiveInWrapMode(t){let e=this._getItemsArray();for(let i=1;i<=e.length;i++){let r=(this._activeItemIndex()+t*i+e.length)%e.length,o=e[r];if(!this._skipPredicateFn(o)){this.setActiveItem(r);return}}}_setActiveInDefaultMode(t){this._setActiveItemByIndex(this._activeItemIndex()+t,t)}_setActiveItemByIndex(t,e){let i=this._getItemsArray();if(i[t]){for(;this._skipPredicateFn(i[t]);)if(t+=e,!i[t])return;this.setActiveItem(t)}}_getItemsArray(){return Hn(this._items)?this._items():this._items instanceof Pi?this._items.toArray():this._items}_itemsChanged(t){this._typeahead?.setItems(t);let e=this._activeItem();if(e){let i=t.indexOf(e);i>-1&&i!==this._activeItemIndex()&&(this._activeItemIndex.set(i),this._typeahead?.setCurrentSelectedItemIndex(i))}}};var tl=class extends ju{setActiveItem(t){this.activeItem&&this.activeItem.setInactiveStyles(),super.setActiveItem(t),this.activeItem&&this.activeItem.setActiveStyles()}};var c_={},Ie=class n{_appId=u(Bi);static _infix=`a${Math.floor(Math.random()*1e5).toString()}`;getId(t,e=!1){return this._appId!=="ng"&&(t+=this._appId),c_.hasOwnProperty(t)||(c_[t]=0),`${t}${e?n._infix+"-":""}${c_[t]++}`}static \u0275fac=function(e){return new(e||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})};var Rx=" ";function m_(n,t,e){let i=$u(n,t);e=e.trim(),!i.some(r=>r.trim()===e)&&(i.push(e),n.setAttribute(t,i.join(Rx)))}function Gu(n,t,e){let i=$u(n,t);e=e.trim();let r=i.filter(o=>o!==e);r.length?n.setAttribute(t,r.join(Rx)):n.removeAttribute(t)}function $u(n,t){return n.getAttribute(t)?.match(/\S+/g)??[]}var Ox="cdk-describedby-message",Uu="cdk-describedby-host",u_=0,h_=(()=>{class n{_platform=u(_e);_document=u(U);_messageRegistry=new Map;_messagesContainer=null;_id=`${u_++}`;constructor(){u(it).load(is),this._id=u(Bi)+"-"+u_++}describe(e,i,r){if(!this._canBeDescribed(e,i))return;let o=d_(i,r);typeof i!="string"?(Ax(i,this._id),this._messageRegistry.set(o,{messageElement:i,referenceCount:0})):this._messageRegistry.has(o)||this._createMessageElement(i,r),this._isElementDescribedByMessage(e,o)||this._addMessageReference(e,o)}removeDescription(e,i,r){if(!i||!this._isElementNode(e))return;let o=d_(i,r);if(this._isElementDescribedByMessage(e,o)&&this._removeMessageReference(e,o),typeof i=="string"){let s=this._messageRegistry.get(o);s&&s.referenceCount===0&&this._deleteMessageElement(o)}this._messagesContainer?.childNodes.length===0&&(this._messagesContainer.remove(),this._messagesContainer=null)}ngOnDestroy(){let e=this._document.querySelectorAll(`[${Uu}="${this._id}"]`);for(let i=0;i<e.length;i++)this._removeCdkDescribedByReferenceIds(e[i]),e[i].removeAttribute(Uu);this._messagesContainer?.remove(),this._messagesContainer=null,this._messageRegistry.clear()}_createMessageElement(e,i){let r=this._document.createElement("div");Ax(r,this._id),r.textContent=e,i&&r.setAttribute("role",i),this._createMessagesContainer(),this._messagesContainer.appendChild(r),this._messageRegistry.set(d_(e,i),{messageElement:r,referenceCount:0})}_deleteMessageElement(e){this._messageRegistry.get(e)?.messageElement?.remove(),this._messageRegistry.delete(e)}_createMessagesContainer(){if(this._messagesContainer)return;let e="cdk-describedby-message-container",i=this._document.querySelectorAll(`.${e}[platform="server"]`);for(let o=0;o<i.length;o++)i[o].remove();let r=this._document.createElement("div");r.style.visibility="hidden",r.classList.add(e),r.classList.add("cdk-visually-hidden"),this._platform.isBrowser||r.setAttribute("platform","server"),this._document.body.appendChild(r),this._messagesContainer=r}_removeCdkDescribedByReferenceIds(e){let i=$u(e,"aria-describedby").filter(r=>r.indexOf(Ox)!=0);e.setAttribute("aria-describedby",i.join(" "))}_addMessageReference(e,i){let r=this._messageRegistry.get(i);m_(e,"aria-describedby",r.messageElement.id),e.setAttribute(Uu,this._id),r.referenceCount++}_removeMessageReference(e,i){let r=this._messageRegistry.get(i);r.referenceCount--,Gu(e,"aria-describedby",r.messageElement.id),e.removeAttribute(Uu)}_isElementDescribedByMessage(e,i){let r=$u(e,"aria-describedby"),o=this._messageRegistry.get(i),s=o&&o.messageElement.id;return!!s&&r.indexOf(s)!=-1}_canBeDescribed(e,i){if(!this._isElementNode(e))return!1;if(i&&typeof i=="object")return!0;let r=i==null?"":`${i}`.trim(),o=e.getAttribute("aria-label");return r?!o||o.trim()!==r:!1}_isElementNode(e){return e.nodeType===this._document.ELEMENT_NODE}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function d_(n,t){return typeof n=="string"?`${t||""}/${n}`:n}function Ax(n,t){n.id||(n.id=`${Ox}-${t}-${u_++}`)}var Qr;function Nx(){if(Qr==null){if(typeof document!="object"||!document||typeof Element!="function"||!Element)return Qr=!1,Qr;if(document.documentElement?.style&&"scrollBehavior"in document.documentElement.style)Qr=!0;else{let n=Element.prototype.scrollTo;n?Qr=!/\{\s*\[native code\]\s*\}/.test(n.toString()):Qr=!1}}return Qr}function f_(){return typeof __karma__<"u"&&!!__karma__||typeof jasmine<"u"&&!!jasmine||typeof jest<"u"&&!!jest||typeof Mocha<"u"&&!!Mocha}var as,Fx=["color","button","checkbox","date","datetime-local","email","file","hidden","image","month","number","password","radio","range","reset","search","submit","tel","text","time","url","week"];function p_(){if(as)return as;if(typeof document!="object"||!document)return as=new Set(Fx),as;let n=document.createElement("input");return as=new Set(Fx.filter(t=>(n.setAttribute("type",t),n.type===t))),as}var Px={XSmall:"(max-width: 599.98px)",Small:"(min-width: 600px) and (max-width: 959.98px)",Medium:"(min-width: 960px) and (max-width: 1279.98px)",Large:"(min-width: 1280px) and (max-width: 1919.98px)",XLarge:"(min-width: 1920px)",Handset:"(max-width: 599.98px) and (orientation: portrait), (max-width: 959.98px) and (orientation: landscape)",Tablet:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait), (min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",Web:"(min-width: 840px) and (orientation: portrait), (min-width: 1280px) and (orientation: landscape)",HandsetPortrait:"(max-width: 599.98px) and (orientation: portrait)",TabletPortrait:"(min-width: 600px) and (max-width: 839.98px) and (orientation: portrait)",WebPortrait:"(min-width: 840px) and (orientation: portrait)",HandsetLandscape:"(max-width: 959.98px) and (orientation: landscape)",TabletLandscape:"(min-width: 960px) and (max-width: 1279.98px) and (orientation: landscape)",WebLandscape:"(min-width: 1280px) and (orientation: landscape)"};var JO=new v("MATERIAL_ANIMATIONS"),Bx=null;function g_(){return u(JO,{optional:!0})?.animationsDisabled||u(Vi,{optional:!0})==="NoopAnimations"?"di-disabled":(Bx??=u(os).matchMedia("(prefers-reduced-motion)").matches,Bx?"reduced-motion":"enabled")}function Fe(){return g_()!=="enabled"}function et(n){return n==null?"":typeof n=="string"?n:`${n}px`}function fi(n){return n!=null&&`${n}`!="false"}var dn=(function(n){return n[n.FADING_IN=0]="FADING_IN",n[n.VISIBLE=1]="VISIBLE",n[n.FADING_OUT=2]="FADING_OUT",n[n.HIDDEN=3]="HIDDEN",n})(dn||{}),__=class{_renderer;element;config;_animationForciblyDisabledThroughCss;state=dn.HIDDEN;constructor(t,e,i,r=!1){this._renderer=t,this.element=e,this.config=i,this._animationForciblyDisabledThroughCss=r}fadeOut(){this._renderer.fadeOutRipple(this)}},Vx=ns({passive:!0,capture:!0}),v_=class{_events=new Map;addHandler(t,e,i,r){let o=this._events.get(e);if(o){let s=o.get(i);s?s.add(r):o.set(i,new Set([r]))}else this._events.set(e,new Map([[i,new Set([r])]])),t.runOutsideAngular(()=>{document.addEventListener(e,this._delegateEventHandler,Vx)})}removeHandler(t,e,i){let r=this._events.get(t);if(!r)return;let o=r.get(e);o&&(o.delete(i),o.size===0&&r.delete(e),r.size===0&&(this._events.delete(t),document.removeEventListener(t,this._delegateEventHandler,Vx)))}_delegateEventHandler=t=>{let e=ft(t);e&&this._events.get(t.type)?.forEach((i,r)=>{(r===e||r.contains(e))&&i.forEach(o=>o.handleEvent(t))})}},nl={enterDuration:225,exitDuration:150},eN=800,jx=ns({passive:!0,capture:!0}),zx=["mousedown","touchstart"],Hx=["mouseup","mouseleave","touchend","touchcancel"],tN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["ng-component"]],hostAttrs:["mat-ripple-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.mat-ripple {
  overflow: hidden;
  position: relative;
}
.mat-ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-ripple.mat-ripple-unbounded {
  overflow: visible;
}

.mat-ripple-element {
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
  transition: opacity, transform 0ms cubic-bezier(0, 0, 0.2, 1);
  transform: scale3d(0, 0, 0);
  background-color: var(--mat-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface) 10%, transparent));
}
@media (forced-colors: active) {
  .mat-ripple-element {
    display: none;
  }
}
.cdk-drag-preview .mat-ripple-element, .cdk-drag-placeholder .mat-ripple-element {
  display: none;
}
`],encapsulation:2,changeDetection:0})}return n})(),il=class n{_target;_ngZone;_platform;_containerElement;_triggerElement=null;_isPointerDown=!1;_activeRipples=new Map;_mostRecentTransientRipple=null;_lastTouchStartEvent;_pointerUpEventsRegistered=!1;_containerRect=null;static _eventManager=new v_;constructor(t,e,i,r,o){this._target=t,this._ngZone=e,this._platform=r,r.isBrowser&&(this._containerElement=Wt(i)),o&&o.get(it).load(tN)}fadeInRipple(t,e,i={}){let r=this._containerRect=this._containerRect||this._containerElement.getBoundingClientRect(),o=M(M({},nl),i.animation);i.centered&&(t=r.left+r.width/2,e=r.top+r.height/2);let s=i.radius||nN(t,e,r),a=t-r.left,l=e-r.top,c=o.enterDuration,d=document.createElement("div");d.classList.add("mat-ripple-element"),d.style.left=`${a-s}px`,d.style.top=`${l-s}px`,d.style.height=`${s*2}px`,d.style.width=`${s*2}px`,i.color!=null&&(d.style.backgroundColor=i.color),d.style.transitionDuration=`${c}ms`,this._containerElement.appendChild(d);let m=window.getComputedStyle(d),f=m.transitionProperty,h=m.transitionDuration,p=f==="none"||h==="0s"||h==="0s, 0s"||r.width===0&&r.height===0,b=new __(this,d,i,p);d.style.transform="scale3d(1, 1, 1)",b.state=dn.FADING_IN,i.persistent||(this._mostRecentTransientRipple=b);let C=null;return!p&&(c||o.exitDuration)&&this._ngZone.runOutsideAngular(()=>{let x=()=>{C&&(C.fallbackTimer=null),clearTimeout(ke),this._finishRippleTransition(b)},ee=()=>this._destroyRipple(b),ke=setTimeout(ee,c+100);d.addEventListener("transitionend",x),d.addEventListener("transitioncancel",ee),C={onTransitionEnd:x,onTransitionCancel:ee,fallbackTimer:ke}}),this._activeRipples.set(b,C),(p||!c)&&this._finishRippleTransition(b),b}fadeOutRipple(t){if(t.state===dn.FADING_OUT||t.state===dn.HIDDEN)return;let e=t.element,i=M(M({},nl),t.config.animation);e.style.transitionDuration=`${i.exitDuration}ms`,e.style.opacity="0",t.state=dn.FADING_OUT,(t._animationForciblyDisabledThroughCss||!i.exitDuration)&&this._finishRippleTransition(t)}fadeOutAll(){this._getActiveRipples().forEach(t=>t.fadeOut())}fadeOutAllNonPersistent(){this._getActiveRipples().forEach(t=>{t.config.persistent||t.fadeOut()})}setupTriggerEvents(t){let e=Wt(t);!this._platform.isBrowser||!e||e===this._triggerElement||(this._removeTriggerEvents(),this._triggerElement=e,zx.forEach(i=>{n._eventManager.addHandler(this._ngZone,i,e,this)}))}handleEvent(t){t.type==="mousedown"?this._onMousedown(t):t.type==="touchstart"?this._onTouchStart(t):this._onPointerUp(),this._pointerUpEventsRegistered||(this._ngZone.runOutsideAngular(()=>{Hx.forEach(e=>{this._triggerElement.addEventListener(e,this,jx)})}),this._pointerUpEventsRegistered=!0)}_finishRippleTransition(t){t.state===dn.FADING_IN?this._startFadeOutTransition(t):t.state===dn.FADING_OUT&&this._destroyRipple(t)}_startFadeOutTransition(t){let e=t===this._mostRecentTransientRipple,{persistent:i}=t.config;t.state=dn.VISIBLE,!i&&(!e||!this._isPointerDown)&&t.fadeOut()}_destroyRipple(t){let e=this._activeRipples.get(t)??null;this._activeRipples.delete(t),this._activeRipples.size||(this._containerRect=null),t===this._mostRecentTransientRipple&&(this._mostRecentTransientRipple=null),t.state=dn.HIDDEN,e!==null&&(t.element.removeEventListener("transitionend",e.onTransitionEnd),t.element.removeEventListener("transitioncancel",e.onTransitionCancel),e.fallbackTimer!==null&&clearTimeout(e.fallbackTimer)),t.element.remove()}_onMousedown(t){let e=qa(t),i=this._lastTouchStartEvent&&Date.now()<this._lastTouchStartEvent+eN;!this._target.rippleDisabled&&!e&&!i&&(this._isPointerDown=!0,this.fadeInRipple(t.clientX,t.clientY,this._target.rippleConfig))}_onTouchStart(t){if(!this._target.rippleDisabled&&!Qa(t)){this._lastTouchStartEvent=Date.now(),this._isPointerDown=!0;let e=t.changedTouches;if(e)for(let i=0;i<e.length;i++)this.fadeInRipple(e[i].clientX,e[i].clientY,this._target.rippleConfig)}}_onPointerUp(){this._isPointerDown&&(this._isPointerDown=!1,this._getActiveRipples().forEach(t=>{let e=t.state===dn.VISIBLE||t.config.terminateOnPointerUp&&t.state===dn.FADING_IN;!t.config.persistent&&e&&t.fadeOut()}))}_getActiveRipples(){return Array.from(this._activeRipples.keys())}_removeTriggerEvents(){let t=this._triggerElement;t&&(zx.forEach(e=>n._eventManager.removeHandler(e,t,this)),this._pointerUpEventsRegistered&&(Hx.forEach(e=>t.removeEventListener(e,this,jx)),this._pointerUpEventsRegistered=!1))}};function nN(n,t,e){let i=Math.max(Math.abs(n-e.left),Math.abs(n-e.right)),r=Math.max(Math.abs(t-e.top),Math.abs(t-e.bottom));return Math.sqrt(i*i+r*r)}var b_=new v("mat-ripple-global-options"),ls=(()=>{class n{_elementRef=u(P);_animationsDisabled=Fe();color;unbounded=!1;centered=!1;radius=0;animation;get disabled(){return this._disabled}set disabled(e){e&&this.fadeOutAllNonPersistent(),this._disabled=e,this._setupTriggerEventsIfEnabled()}_disabled=!1;get trigger(){return this._trigger||this._elementRef.nativeElement}set trigger(e){this._trigger=e,this._setupTriggerEventsIfEnabled()}_trigger;_rippleRenderer;_globalOptions;_isInitialized=!1;constructor(){let e=u(F),i=u(_e),r=u(b_,{optional:!0}),o=u($);this._globalOptions=r||{},this._rippleRenderer=new il(this,e,this._elementRef,i,o)}ngOnInit(){this._isInitialized=!0,this._setupTriggerEventsIfEnabled()}ngOnDestroy(){this._rippleRenderer._removeTriggerEvents()}fadeOutAll(){this._rippleRenderer.fadeOutAll()}fadeOutAllNonPersistent(){this._rippleRenderer.fadeOutAllNonPersistent()}get rippleConfig(){return{centered:this.centered,radius:this.radius,color:this.color,animation:M(M(M({},this._globalOptions.animation),this._animationsDisabled?{enterDuration:0,exitDuration:0}:{}),this.animation),terminateOnPointerUp:this._globalOptions.terminateOnPointerUp}}get rippleDisabled(){return this.disabled||!!this._globalOptions.disabled}_setupTriggerEventsIfEnabled(){!this.disabled&&this._isInitialized&&this._rippleRenderer.setupTriggerEvents(this.trigger)}launch(e,i=0,r){return typeof e=="number"?this._rippleRenderer.fadeInRipple(e,i,M(M({},this.rippleConfig),r)):this._rippleRenderer.fadeInRipple(0,0,M(M({},this.rippleConfig),e))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","mat-ripple",""],["","matRipple",""]],hostAttrs:[1,"mat-ripple"],hostVars:2,hostBindings:function(i,r){i&2&&j("mat-ripple-unbounded",r.unbounded)},inputs:{color:[0,"matRippleColor","color"],unbounded:[0,"matRippleUnbounded","unbounded"],centered:[0,"matRippleCentered","centered"],radius:[0,"matRippleRadius","radius"],animation:[0,"matRippleAnimation","animation"],disabled:[0,"matRippleDisabled","disabled"],trigger:[0,"matRippleTrigger","trigger"]},exportAs:["matRipple"]})}return n})();var iN={capture:!0},rN=["focus","mousedown","mouseenter","touchstart"],y_="mat-ripple-loader-uninitialized",w_="mat-ripple-loader-class-name",Ux="mat-ripple-loader-centered",Wu="mat-ripple-loader-disabled",$x=(()=>{class n{_document=u(U);_animationsDisabled=Fe();_globalRippleOptions=u(b_,{optional:!0});_platform=u(_e);_ngZone=u(F);_injector=u($);_eventCleanups;_hosts=new Map;constructor(){let e=u(qe).createRenderer(null,null);this._eventCleanups=this._ngZone.runOutsideAngular(()=>rN.map(i=>e.listen(this._document,i,this._onInteraction,iN)))}ngOnDestroy(){let e=this._hosts.keys();for(let i of e)this.destroyRipple(i);this._eventCleanups.forEach(i=>i())}configureRipple(e,i){e.setAttribute(y_,this._globalRippleOptions?.namespace??""),(i.className||!e.hasAttribute(w_))&&e.setAttribute(w_,i.className||""),i.centered&&e.setAttribute(Ux,""),i.disabled&&e.setAttribute(Wu,"")}setDisabled(e,i){let r=this._hosts.get(e);r?(r.target.rippleDisabled=i,!i&&!r.hasSetUpEvents&&(r.hasSetUpEvents=!0,r.renderer.setupTriggerEvents(e))):i?e.setAttribute(Wu,""):e.removeAttribute(Wu)}_onInteraction=e=>{let i=ft(e);if(i instanceof HTMLElement){let r=i.closest(`[${y_}="${this._globalRippleOptions?.namespace??""}"]`);r&&this._createRipple(r)}};_createRipple(e){if(!this._document||this._hosts.has(e))return;e.querySelector(".mat-ripple")?.remove();let i=this._document.createElement("span");i.classList.add("mat-ripple",e.getAttribute(w_)),e.append(i);let r=this._globalRippleOptions,o=this._animationsDisabled?0:r?.animation?.enterDuration??nl.enterDuration,s=this._animationsDisabled?0:r?.animation?.exitDuration??nl.exitDuration,a={rippleDisabled:this._animationsDisabled||r?.disabled||e.hasAttribute(Wu),rippleConfig:{centered:e.hasAttribute(Ux),terminateOnPointerUp:r?.terminateOnPointerUp,animation:{enterDuration:o,exitDuration:s}}},l=new il(a,this._ngZone,i,this._platform,this._injector),c=!a.rippleDisabled;c&&l.setupTriggerEvents(e),this._hosts.set(e,{target:a,renderer:l,hasSetUpEvents:c}),e.removeAttribute(y_)}destroyRipple(e){let i=this._hosts.get(e);i&&(i.renderer._removeTriggerEvents(),this._hosts.delete(e))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var qi=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["structural-styles"]],decls:0,vars:0,template:function(i,r){},styles:[`.mat-focus-indicator {
  position: relative;
}
.mat-focus-indicator::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  box-sizing: border-box;
  pointer-events: none;
  display: var(--mat-focus-indicator-display, none);
  border-width: var(--mat-focus-indicator-border-width, 3px);
  border-style: var(--mat-focus-indicator-border-style, solid);
  border-color: var(--mat-focus-indicator-border-color, transparent);
  border-radius: var(--mat-focus-indicator-border-radius, 4px);
}
.mat-focus-indicator:focus-visible::before {
  content: "";
}

@media (forced-colors: active) {
  html {
    --mat-focus-indicator-display: block;
  }
}
`],encapsulation:2,changeDetection:0})}return n})();var oN=["mat-icon-button",""],sN=["*"],aN=new v("MAT_BUTTON_CONFIG");function Gx(n){return n==null?void 0:ht(n)}var C_=(()=>{class n{_elementRef=u(P);_ngZone=u(F);_animationsDisabled=Fe();_config=u(aN,{optional:!0});_focusMonitor=u(mi);_cleanupClick;_renderer=u(Ke);_rippleLoader=u($x);_isAnchor;_isFab=!1;color;get disableRipple(){return this._disableRipple}set disableRipple(e){this._disableRipple=e,this._updateRippleDisabled()}_disableRipple=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._updateRippleDisabled()}_disabled=!1;ariaDisabled;disabledInteractive;tabIndex;set _tabindex(e){this.tabIndex=e}constructor(){u(it).load(qi);let e=this._elementRef.nativeElement;this._isAnchor=e.tagName==="A",this.disabledInteractive=this._config?.disabledInteractive??!1,this.color=this._config?.color??null,this._rippleLoader?.configureRipple(e,{className:"mat-mdc-button-ripple"})}ngAfterViewInit(){this._focusMonitor.monitor(this._elementRef,!0),this._isAnchor&&this._setupAsAnchor()}ngOnDestroy(){this._cleanupClick?.(),this._focusMonitor.stopMonitoring(this._elementRef),this._rippleLoader?.destroyRipple(this._elementRef.nativeElement)}focus(e="program",i){e?this._focusMonitor.focusVia(this._elementRef.nativeElement,e,i):this._elementRef.nativeElement.focus(i)}_getAriaDisabled(){return this.ariaDisabled!=null?this.ariaDisabled:this._isAnchor?this.disabled||null:this.disabled&&this.disabledInteractive?!0:null}_getDisabledAttribute(){return this.disabledInteractive||!this.disabled?null:!0}_updateRippleDisabled(){this._rippleLoader?.setDisabled(this._elementRef.nativeElement,this.disableRipple||this.disabled)}_getTabIndex(){return this._isAnchor?this.disabled&&!this.disabledInteractive?-1:this.tabIndex:this.tabIndex}_setupAsAnchor(){this._cleanupClick=this._ngZone.runOutsideAngular(()=>this._renderer.listen(this._elementRef.nativeElement,"click",e=>{this.disabled&&(e.preventDefault(),e.stopImmediatePropagation())}))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,hostAttrs:[1,"mat-mdc-button-base"],hostVars:13,hostBindings:function(i,r){i&2&&(J("disabled",r._getDisabledAttribute())("aria-disabled",r._getAriaDisabled())("tabindex",r._getTabIndex()),St(r.color?"mat-"+r.color:""),j("mat-mdc-button-disabled",r.disabled)("mat-mdc-button-disabled-interactive",r.disabledInteractive)("mat-unthemed",!r.color)("_mat-animation-noopable",r._animationsDisabled))},inputs:{color:"color",disableRipple:[2,"disableRipple","disableRipple",N],disabled:[2,"disabled","disabled",N],ariaDisabled:[2,"aria-disabled","ariaDisabled",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N],tabIndex:[2,"tabIndex","tabIndex",Gx],_tabindex:[2,"tabindex","_tabindex",Gx]}})}return n})(),D_=(()=>{class n extends C_{constructor(){super(),this._rippleLoader.configureRipple(this._elementRef.nativeElement,{centered:!0})}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["button","mat-icon-button",""],["a","mat-icon-button",""],["button","matIconButton",""],["a","matIconButton",""]],hostAttrs:[1,"mdc-icon-button","mat-mdc-icon-button"],exportAs:["matButton","matAnchor"],features:[re],attrs:oN,ngContentSelectors:sN,decls:4,vars:0,consts:[[1,"mat-mdc-button-persistent-ripple","mdc-icon-button__ripple"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ee(),on(0,"span",0),ie(1),on(2,"span",1)(3,"span",2))},styles:[`.mat-mdc-icon-button {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  position: relative;
  box-sizing: border-box;
  border: none;
  outline: none;
  background-color: transparent;
  fill: currentColor;
  text-decoration: none;
  cursor: pointer;
  z-index: 0;
  overflow: visible;
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
  flex-shrink: 0;
  text-align: center;
  width: var(--mat-icon-button-state-layer-size, 40px);
  height: var(--mat-icon-button-state-layer-size, 40px);
  padding: calc(calc(var(--mat-icon-button-state-layer-size, 40px) - var(--mat-icon-button-icon-size, 24px)) / 2);
  font-size: var(--mat-icon-button-icon-size, 24px);
  color: var(--mat-icon-button-icon-color, var(--mat-sys-on-surface-variant));
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-icon-button .mat-mdc-button-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple,
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-icon-button .mdc-button__label,
.mat-mdc-icon-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-icon-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-icon-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-icon-button .mat-ripple-element {
  background-color: var(--mat-icon-button-ripple-color, color-mix(in srgb, var(--mat-sys-on-surface-variant) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-icon-button-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-icon-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-icon-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-icon-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-icon-button-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-icon-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-icon-button-touch-target-size, 48px);
  display: var(--mat-icon-button-touch-target-display, block);
  left: 50%;
  width: var(--mat-icon-button-touch-target-size, 48px);
  transform: translate(-50%, -50%);
}
.mat-mdc-icon-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-icon-button[disabled], .mat-mdc-icon-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-icon-button-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-icon-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-icon-button img,
.mat-mdc-icon-button svg {
  width: var(--mat-icon-button-icon-size, 24px);
  height: var(--mat-icon-button-icon-size, 24px);
  vertical-align: baseline;
}
.mat-mdc-icon-button .mat-mdc-button-persistent-ripple {
  border-radius: var(--mat-icon-button-container-shape, var(--mat-sys-corner-full, 50%));
}
.mat-mdc-icon-button[hidden] {
  display: none;
}
.mat-mdc-icon-button.mat-unthemed:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-primary:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-accent:not(.mdc-ripple-upgraded):focus::before, .mat-mdc-icon-button.mat-warn:not(.mdc-ripple-upgraded):focus::before {
  background: transparent;
  opacity: 1;
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return n})();var lN=new v("cdk-dir-doc",{providedIn:"root",factory:()=>u(U)}),cN=/^(ar|ckb|dv|he|iw|fa|nqo|ps|sd|ug|ur|yi|.*[-_](Adlm|Arab|Hebr|Nkoo|Rohg|Thaa))(?!.*[-_](Latn|Cyrl)($|-|_))($|-|_)/i;function Wx(n){let t=n?.toLowerCase()||"";return t==="auto"&&typeof navigator<"u"&&navigator?.language?cN.test(navigator.language)?"rtl":"ltr":t==="rtl"?"rtl":"ltr"}var st=(()=>{class n{get value(){return this.valueSignal()}valueSignal=Oe("ltr");change=new z;constructor(){let e=u(lN,{optional:!0});if(e){let i=e.body?e.body.dir:null,r=e.documentElement?e.documentElement.dir:null;this.valueSignal.set(Wx(i||r||"ltr"))}}ngOnDestroy(){this.change.complete()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var oe=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})();var Qi=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe]})}return n})();var dN=["matButton",""],uN=[[["",8,"material-icons",3,"iconPositionEnd",""],["mat-icon",3,"iconPositionEnd",""],["","matButtonIcon","",3,"iconPositionEnd",""]],"*",[["","iconPositionEnd","",8,"material-icons"],["mat-icon","iconPositionEnd",""],["","matButtonIcon","","iconPositionEnd",""]]],mN=[".material-icons:not([iconPositionEnd]), mat-icon:not([iconPositionEnd]), [matButtonIcon]:not([iconPositionEnd])","*",".material-icons[iconPositionEnd], mat-icon[iconPositionEnd], [matButtonIcon][iconPositionEnd]"];var qx=new Map([["text",["mat-mdc-button"]],["filled",["mdc-button--unelevated","mat-mdc-unelevated-button"]],["elevated",["mdc-button--raised","mat-mdc-raised-button"]],["outlined",["mdc-button--outlined","mat-mdc-outlined-button"]],["tonal",["mat-tonal-button"]]]),Gn=(()=>{class n extends C_{get appearance(){return this._appearance}set appearance(e){this.setAppearance(e||this._config?.defaultAppearance||"text")}_appearance=null;constructor(){super();let e=hN(this._elementRef.nativeElement);e&&this.setAppearance(e)}setAppearance(e){if(e===this._appearance)return;let i=this._elementRef.nativeElement.classList,r=this._appearance?qx.get(this._appearance):null,o=qx.get(e);r&&i.remove(...r),i.add(...o),this._appearance=e}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["button","matButton",""],["a","matButton",""],["button","mat-button",""],["button","mat-raised-button",""],["button","mat-flat-button",""],["button","mat-stroked-button",""],["a","mat-button",""],["a","mat-raised-button",""],["a","mat-flat-button",""],["a","mat-stroked-button",""]],hostAttrs:[1,"mdc-button"],inputs:{appearance:[0,"matButton","appearance"]},exportAs:["matButton","matAnchor"],features:[re],attrs:dN,ngContentSelectors:mN,decls:7,vars:4,consts:[[1,"mat-mdc-button-persistent-ripple"],[1,"mdc-button__label"],[1,"mat-focus-indicator"],[1,"mat-mdc-button-touch-target"]],template:function(i,r){i&1&&(Ee(uN),on(0,"span",0),ie(1),Ot(2,"span",1),ie(3,1),$t(),ie(4,2),on(5,"span",2)(6,"span",3)),i&2&&j("mdc-button__ripple",!r._isFab)("mdc-fab__ripple",r._isFab)},styles:[`.mat-mdc-button-base {
  text-decoration: none;
}
.mat-mdc-button-base .mat-icon {
  min-height: fit-content;
  flex-shrink: 0;
}
@media (hover: none) {
  .mat-mdc-button-base:hover > span.mat-mdc-button-persistent-ripple::before {
    opacity: 0;
  }
}

.mdc-button {
  -webkit-user-select: none;
  user-select: none;
  position: relative;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  min-width: 64px;
  border: none;
  outline: none;
  line-height: inherit;
  -webkit-appearance: none;
  overflow: visible;
  vertical-align: middle;
  background: transparent;
  padding: 0 8px;
}
.mdc-button::-moz-focus-inner {
  padding: 0;
  border: 0;
}
.mdc-button:active {
  outline: none;
}
.mdc-button:hover {
  cursor: pointer;
}
.mdc-button:disabled {
  cursor: default;
  pointer-events: none;
}
.mdc-button[hidden] {
  display: none;
}
.mdc-button .mdc-button__label {
  position: relative;
}

.mat-mdc-button {
  padding: 0 var(--mat-button-text-horizontal-padding, 12px);
  height: var(--mat-button-text-container-height, 40px);
  font-family: var(--mat-button-text-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-text-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-text-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-text-label-text-transform);
  font-weight: var(--mat-button-text-label-text-weight, var(--mat-sys-label-large-weight));
}
.mat-mdc-button, .mat-mdc-button .mdc-button__ripple {
  border-radius: var(--mat-button-text-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-button:not(:disabled) {
  color: var(--mat-button-text-label-text-color, var(--mat-sys-primary));
}
.mat-mdc-button[disabled], .mat-mdc-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-text-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-button:has(.material-icons, mat-icon, [matButtonIcon]) {
  padding: 0 var(--mat-button-text-with-icon-horizontal-padding, 16px);
}
.mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
[dir=rtl] .mat-mdc-button > .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
.mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-offset, -4px);
  margin-left: var(--mat-button-text-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-text-icon-spacing, 8px);
  margin-left: var(--mat-button-text-icon-offset, -4px);
}
.mat-mdc-button .mat-ripple-element {
  background-color: var(--mat-button-text-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-text-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-text-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-text-touch-target-size, 48px);
  display: var(--mat-button-text-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-unelevated-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-filled-container-height, 40px);
  font-family: var(--mat-button-filled-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-filled-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-filled-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-filled-label-text-transform);
  font-weight: var(--mat-button-filled-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-filled-horizontal-padding, 24px);
}
.mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-unelevated-button > .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
.mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-offset, -8px);
  margin-left: var(--mat-button-filled-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-unelevated-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-filled-icon-spacing, 8px);
  margin-left: var(--mat-button-filled-icon-offset, -8px);
}
.mat-mdc-unelevated-button .mat-ripple-element {
  background-color: var(--mat-button-filled-ripple-color, color-mix(in srgb, var(--mat-sys-on-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-state-layer-color, var(--mat-sys-on-primary));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-filled-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-unelevated-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-unelevated-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-unelevated-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-filled-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-unelevated-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-filled-touch-target-size, 48px);
  display: var(--mat-button-filled-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-unelevated-button:not(:disabled) {
  color: var(--mat-button-filled-label-text-color, var(--mat-sys-on-primary));
  background-color: var(--mat-button-filled-container-color, var(--mat-sys-primary));
}
.mat-mdc-unelevated-button, .mat-mdc-unelevated-button .mdc-button__ripple {
  border-radius: var(--mat-button-filled-container-shape, var(--mat-sys-corner-full));
}
.mat-mdc-unelevated-button[disabled], .mat-mdc-unelevated-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-unelevated-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-raised-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  box-shadow: var(--mat-button-protected-container-elevation-shadow, var(--mat-sys-level1));
  height: var(--mat-button-protected-container-height, 40px);
  font-family: var(--mat-button-protected-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-protected-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-protected-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-protected-label-text-transform);
  font-weight: var(--mat-button-protected-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-protected-horizontal-padding, 24px);
}
.mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-raised-button > .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
.mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-offset, -8px);
  margin-left: var(--mat-button-protected-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-raised-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-protected-icon-spacing, 8px);
  margin-left: var(--mat-button-protected-icon-offset, -8px);
}
.mat-mdc-raised-button .mat-ripple-element {
  background-color: var(--mat-button-protected-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-raised-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-protected-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-raised-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-raised-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-raised-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-raised-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-protected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-raised-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-protected-touch-target-size, 48px);
  display: var(--mat-button-protected-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-raised-button:not(:disabled) {
  color: var(--mat-button-protected-label-text-color, var(--mat-sys-primary));
  background-color: var(--mat-button-protected-container-color, var(--mat-sys-surface));
}
.mat-mdc-raised-button, .mat-mdc-raised-button .mdc-button__ripple {
  border-radius: var(--mat-button-protected-container-shape, var(--mat-sys-corner-full));
}
@media (hover: hover) {
  .mat-mdc-raised-button:hover {
    box-shadow: var(--mat-button-protected-hover-container-elevation-shadow, var(--mat-sys-level2));
  }
}
.mat-mdc-raised-button:focus {
  box-shadow: var(--mat-button-protected-focus-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button:active, .mat-mdc-raised-button:focus:active {
  box-shadow: var(--mat-button-protected-pressed-container-elevation-shadow, var(--mat-sys-level1));
}
.mat-mdc-raised-button[disabled], .mat-mdc-raised-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-protected-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-protected-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-raised-button[disabled].mat-mdc-button-disabled, .mat-mdc-raised-button.mat-mdc-button-disabled.mat-mdc-button-disabled {
  box-shadow: var(--mat-button-protected-disabled-container-elevation-shadow, var(--mat-sys-level0));
}
.mat-mdc-raised-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-mdc-outlined-button {
  border-style: solid;
  transition: border 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-outlined-container-height, 40px);
  font-family: var(--mat-button-outlined-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-outlined-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-outlined-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-outlined-label-text-transform);
  font-weight: var(--mat-button-outlined-label-text-weight, var(--mat-sys-label-large-weight));
  border-radius: var(--mat-button-outlined-container-shape, var(--mat-sys-corner-full));
  border-width: var(--mat-button-outlined-outline-width, 1px);
  padding: 0 var(--mat-button-outlined-horizontal-padding, 24px);
}
.mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
[dir=rtl] .mat-mdc-outlined-button > .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
.mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-offset, -8px);
  margin-left: var(--mat-button-outlined-icon-spacing, 8px);
}
[dir=rtl] .mat-mdc-outlined-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-outlined-icon-spacing, 8px);
  margin-left: var(--mat-button-outlined-icon-offset, -8px);
}
.mat-mdc-outlined-button .mat-ripple-element {
  background-color: var(--mat-button-outlined-ripple-color, color-mix(in srgb, var(--mat-sys-primary) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-state-layer-color, var(--mat-sys-primary));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-outlined-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-outlined-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-outlined-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-mdc-outlined-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-mdc-outlined-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-outlined-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-mdc-outlined-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-outlined-touch-target-size, 48px);
  display: var(--mat-button-outlined-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}
.mat-mdc-outlined-button:not(:disabled) {
  color: var(--mat-button-outlined-label-text-color, var(--mat-sys-primary));
  border-color: var(--mat-button-outlined-outline-color, var(--mat-sys-outline));
}
.mat-mdc-outlined-button[disabled], .mat-mdc-outlined-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: var(--mat-button-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-mdc-outlined-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}

.mat-tonal-button {
  transition: box-shadow 280ms cubic-bezier(0.4, 0, 0.2, 1);
  height: var(--mat-button-tonal-container-height, 40px);
  font-family: var(--mat-button-tonal-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-tonal-label-text-size, var(--mat-sys-label-large-size));
  letter-spacing: var(--mat-button-tonal-label-text-tracking, var(--mat-sys-label-large-tracking));
  text-transform: var(--mat-button-tonal-label-text-transform);
  font-weight: var(--mat-button-tonal-label-text-weight, var(--mat-sys-label-large-weight));
  padding: 0 var(--mat-button-tonal-horizontal-padding, 24px);
}
.mat-tonal-button:not(:disabled) {
  color: var(--mat-button-tonal-label-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-tonal-container-color, var(--mat-sys-secondary-container));
}
.mat-tonal-button, .mat-tonal-button .mdc-button__ripple {
  border-radius: var(--mat-button-tonal-container-shape, var(--mat-sys-corner-full));
}
.mat-tonal-button[disabled], .mat-tonal-button.mat-mdc-button-disabled {
  cursor: default;
  pointer-events: none;
  color: var(--mat-button-tonal-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-tonal-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-tonal-button.mat-mdc-button-disabled-interactive {
  pointer-events: auto;
}
.mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
[dir=rtl] .mat-tonal-button > .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
.mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-offset, -8px);
  margin-left: var(--mat-button-tonal-icon-spacing, 8px);
}
[dir=rtl] .mat-tonal-button .mdc-button__label + .mat-icon {
  margin-right: var(--mat-button-tonal-icon-spacing, 8px);
  margin-left: var(--mat-button-tonal-icon-offset, -8px);
}
.mat-tonal-button .mat-ripple-element {
  background-color: var(--mat-button-tonal-ripple-color, color-mix(in srgb, var(--mat-sys-on-secondary-container) calc(var(--mat-sys-pressed-state-layer-opacity) * 100%), transparent));
}
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-state-layer-color, var(--mat-sys-on-secondary-container));
}
.mat-tonal-button.mat-mdc-button-disabled .mat-mdc-button-persistent-ripple::before {
  background-color: var(--mat-button-tonal-disabled-state-layer-color, var(--mat-sys-on-surface-variant));
}
.mat-tonal-button:hover > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-tonal-button.cdk-program-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.cdk-keyboard-focused > .mat-mdc-button-persistent-ripple::before, .mat-tonal-button.mat-mdc-button-disabled-interactive:focus > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
.mat-tonal-button:active > .mat-mdc-button-persistent-ripple::before {
  opacity: var(--mat-button-tonal-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
}
.mat-tonal-button .mat-mdc-button-touch-target {
  position: absolute;
  top: 50%;
  height: var(--mat-button-tonal-touch-target-size, 48px);
  display: var(--mat-button-tonal-touch-target-display, block);
  left: 0;
  right: 0;
  transform: translateY(-50%);
}

.mat-mdc-button,
.mat-mdc-unelevated-button,
.mat-mdc-raised-button,
.mat-mdc-outlined-button,
.mat-tonal-button {
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple,
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
  border-radius: inherit;
}
.mat-mdc-button .mat-mdc-button-ripple,
.mat-mdc-unelevated-button .mat-mdc-button-ripple,
.mat-mdc-raised-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-tonal-button .mat-mdc-button-ripple {
  overflow: hidden;
}
.mat-mdc-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-unelevated-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-raised-button .mat-mdc-button-persistent-ripple::before,
.mat-mdc-outlined-button .mat-mdc-button-persistent-ripple::before,
.mat-tonal-button .mat-mdc-button-persistent-ripple::before {
  content: "";
  opacity: 0;
}
.mat-mdc-button .mdc-button__label,
.mat-mdc-button .mat-icon,
.mat-mdc-unelevated-button .mdc-button__label,
.mat-mdc-unelevated-button .mat-icon,
.mat-mdc-raised-button .mdc-button__label,
.mat-mdc-raised-button .mat-icon,
.mat-mdc-outlined-button .mdc-button__label,
.mat-mdc-outlined-button .mat-icon,
.mat-tonal-button .mdc-button__label,
.mat-tonal-button .mat-icon {
  z-index: 1;
  position: relative;
}
.mat-mdc-button .mat-focus-indicator,
.mat-mdc-unelevated-button .mat-focus-indicator,
.mat-mdc-raised-button .mat-focus-indicator,
.mat-mdc-outlined-button .mat-focus-indicator,
.mat-tonal-button .mat-focus-indicator {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
}
.mat-mdc-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-unelevated-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-raised-button:focus-visible > .mat-focus-indicator::before,
.mat-mdc-outlined-button:focus-visible > .mat-focus-indicator::before,
.mat-tonal-button:focus-visible > .mat-focus-indicator::before {
  content: "";
  border-radius: inherit;
}
.mat-mdc-button._mat-animation-noopable,
.mat-mdc-unelevated-button._mat-animation-noopable,
.mat-mdc-raised-button._mat-animation-noopable,
.mat-mdc-outlined-button._mat-animation-noopable,
.mat-tonal-button._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-button > .mat-icon,
.mat-mdc-unelevated-button > .mat-icon,
.mat-mdc-raised-button > .mat-icon,
.mat-mdc-outlined-button > .mat-icon,
.mat-tonal-button > .mat-icon {
  display: inline-block;
  position: relative;
  vertical-align: top;
  font-size: 1.125rem;
  height: 1.125rem;
  width: 1.125rem;
}

.mat-mdc-outlined-button .mat-mdc-button-ripple,
.mat-mdc-outlined-button .mdc-button__ripple {
  top: -1px;
  left: -1px;
  bottom: -1px;
  right: -1px;
}

.mat-mdc-unelevated-button .mat-focus-indicator::before,
.mat-tonal-button .mat-focus-indicator::before,
.mat-mdc-raised-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 2px) * -1);
}

.mat-mdc-outlined-button .mat-focus-indicator::before {
  margin: calc(calc(var(--mat-focus-indicator-border-width, 3px) + 3px) * -1);
}
`,`@media (forced-colors: active) {
  .mat-mdc-button:not(.mdc-button--outlined),
  .mat-mdc-unelevated-button:not(.mdc-button--outlined),
  .mat-mdc-raised-button:not(.mdc-button--outlined),
  .mat-mdc-outlined-button:not(.mdc-button--outlined),
  .mat-mdc-button-base.mat-tonal-button,
  .mat-mdc-icon-button.mat-mdc-icon-button,
  .mat-mdc-outlined-button .mdc-button__ripple {
    outline: solid 1px;
  }
}
`],encapsulation:2,changeDetection:0})}return n})();function hN(n){return n.hasAttribute("mat-raised-button")?"elevated":n.hasAttribute("mat-stroked-button")?"outlined":n.hasAttribute("mat-flat-button")?"filled":n.hasAttribute("mat-button")?"text":null}var Ki=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[Qi,oe]})}return n})();var x_=class{_box;_destroyed=new k;_resizeSubject=new k;_resizeObserver;_elementObservables=new Map;constructor(t){this._box=t,typeof ResizeObserver<"u"&&(this._resizeObserver=new ResizeObserver(e=>this._resizeSubject.next(e)))}observe(t){return this._elementObservables.has(t)||this._elementObservables.set(t,new ae(e=>{let i=this._resizeSubject.subscribe(e);return this._resizeObserver?.observe(t,{box:this._box}),()=>{this._resizeObserver?.unobserve(t),i.unsubscribe(),this._elementObservables.delete(t)}}).pipe(ze(e=>e.some(i=>i.target===t)),pc({bufferSize:1,refCount:!0}),Ce(this._destroyed))),this._elementObservables.get(t)}destroy(){this._destroyed.next(),this._destroyed.complete(),this._resizeSubject.complete(),this._elementObservables.clear()}},Qx=(()=>{class n{_cleanupErrorListener;_observers=new Map;_ngZone=u(F);constructor(){typeof ResizeObserver<"u"}ngOnDestroy(){for(let[,e]of this._observers)e.destroy();this._observers.clear(),this._cleanupErrorListener?.()}observe(e,i){let r=i?.box||"content-box";return this._observers.has(r)||this._observers.set(r,new x_(r)),this._observers.get(r).observe(e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var fN=["notch"],pN=["matFormFieldNotchedOutline",""],gN=["*"],Kx=["iconPrefixContainer"],Yx=["textPrefixContainer"],Zx=["iconSuffixContainer"],Xx=["textSuffixContainer"],_N=["textField"],vN=["*",[["mat-label"]],[["","matPrefix",""],["","matIconPrefix",""]],[["","matTextPrefix",""]],[["","matTextSuffix",""]],[["","matSuffix",""],["","matIconSuffix",""]],[["mat-error"],["","matError",""]],[["mat-hint",3,"align","end"]],[["mat-hint","align","end"]]],bN=["*","mat-label","[matPrefix], [matIconPrefix]","[matTextPrefix]","[matTextSuffix]","[matSuffix], [matIconSuffix]","mat-error, [matError]","mat-hint:not([align='end'])","mat-hint[align='end']"];function yN(n,t){n&1&&Z(0,"span",21)}function wN(n,t){if(n&1&&(_(0,"label",20),ie(1,1),ce(2,yN,1,0,"span",21),g()),n&2){let e=H(2);R("floating",e._shouldLabelFloat())("monitorResize",e._hasOutline())("id",e._labelId),J("for",e._control.disableAutomaticLabeling?null:e._control.id),y(2),de(!e.hideRequiredMarker&&e._control.required?2:-1)}}function CN(n,t){if(n&1&&ce(0,wN,3,5,"label",20),n&2){let e=H();de(e._hasFloatingLabel()?0:-1)}}function DN(n,t){n&1&&Z(0,"div",7)}function xN(n,t){}function EN(n,t){if(n&1&&fe(0,xN,0,0,"ng-template",13),n&2){H(2);let e=dt(1);R("ngTemplateOutlet",e)}}function kN(n,t){if(n&1&&(_(0,"div",9),ce(1,EN,1,1,null,13),g()),n&2){let e=H();R("matFormFieldNotchedOutlineOpen",e._shouldLabelFloat()),y(),de(e._forceDisplayInfixLabel()?-1:1)}}function SN(n,t){n&1&&(_(0,"div",10,2),ie(2,2),g())}function IN(n,t){n&1&&(_(0,"div",11,3),ie(2,3),g())}function MN(n,t){}function TN(n,t){if(n&1&&fe(0,MN,0,0,"ng-template",13),n&2){H();let e=dt(1);R("ngTemplateOutlet",e)}}function AN(n,t){n&1&&(_(0,"div",14,4),ie(2,4),g())}function RN(n,t){n&1&&(_(0,"div",15,5),ie(2,5),g())}function ON(n,t){n&1&&Z(0,"div",16)}function NN(n,t){n&1&&(_(0,"div",18),ie(1,6),g())}function FN(n,t){if(n&1&&(_(0,"mat-hint",22),E(1),g()),n&2){let e=H(2);R("id",e._hintLabelId),y(),It(e.hintLabel)}}function PN(n,t){if(n&1&&(_(0,"div",19),ce(1,FN,2,2,"mat-hint",22),ie(2,7),Z(3,"div",23),ie(4,8),g()),n&2){let e=H();y(),de(e.hintLabel?1:-1)}}var pi=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["mat-label"]]})}return n})(),oE=new v("MatError"),k_=(()=>{class n{id=u(Ie).getId("mat-mdc-error-");constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["mat-error"],["","matError",""]],hostAttrs:[1,"mat-mdc-form-field-error","mat-mdc-form-field-bottom-align"],hostVars:1,hostBindings:function(i,r){i&2&&Nt("id",r.id)},inputs:{id:"id"},features:[ve([{provide:oE,useExisting:n}])]})}return n})(),E_=(()=>{class n{align="start";id=u(Ie).getId("mat-mdc-hint-");static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["mat-hint"]],hostAttrs:[1,"mat-mdc-form-field-hint","mat-mdc-form-field-bottom-align"],hostVars:4,hostBindings:function(i,r){i&2&&(Nt("id",r.id),J("align",null),j("mat-mdc-form-field-hint-end",r.align==="end"))},inputs:{align:"align",id:"id"}})}return n})(),LN=new v("MatPrefix");var BN=new v("MatSuffix");var sE=new v("FloatingLabelParent"),Jx=(()=>{class n{_elementRef=u(P);get floating(){return this._floating}set floating(e){this._floating=e,this.monitorResize&&this._handleResize()}_floating=!1;get monitorResize(){return this._monitorResize}set monitorResize(e){this._monitorResize=e,this._monitorResize?this._subscribeToResize():this._resizeSubscription.unsubscribe()}_monitorResize=!1;_resizeObserver=u(Qx);_ngZone=u(F);_parent=u(sE);_resizeSubscription=new ge;constructor(){}ngOnDestroy(){this._resizeSubscription.unsubscribe()}getWidth(){return VN(this._elementRef.nativeElement)}get element(){return this._elementRef.nativeElement}_handleResize(){setTimeout(()=>this._parent._handleLabelResized())}_subscribeToResize(){this._resizeSubscription.unsubscribe(),this._ngZone.runOutsideAngular(()=>{this._resizeSubscription=this._resizeObserver.observe(this._elementRef.nativeElement,{box:"border-box"}).subscribe(()=>this._handleResize())})}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["label","matFormFieldFloatingLabel",""]],hostAttrs:[1,"mdc-floating-label","mat-mdc-floating-label"],hostVars:2,hostBindings:function(i,r){i&2&&j("mdc-floating-label--float-above",r.floating)},inputs:{floating:"floating",monitorResize:"monitorResize"}})}return n})();function VN(n){let t=n;if(t.offsetParent!==null)return t.scrollWidth;let e=t.cloneNode(!0);e.style.setProperty("position","absolute"),e.style.setProperty("transform","translate(-9999px, -9999px)"),document.documentElement.appendChild(e);let i=e.scrollWidth;return e.remove(),i}var eE="mdc-line-ripple--active",Qu="mdc-line-ripple--deactivating",tE=(()=>{class n{_elementRef=u(P);_cleanupTransitionEnd;constructor(){let e=u(F),i=u(Ke);e.runOutsideAngular(()=>{this._cleanupTransitionEnd=i.listen(this._elementRef.nativeElement,"transitionend",this._handleTransitionEnd)})}activate(){let e=this._elementRef.nativeElement.classList;e.remove(Qu),e.add(eE)}deactivate(){this._elementRef.nativeElement.classList.add(Qu)}_handleTransitionEnd=e=>{let i=this._elementRef.nativeElement.classList,r=i.contains(Qu);e.propertyName==="opacity"&&r&&i.remove(eE,Qu)};ngOnDestroy(){this._cleanupTransitionEnd()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["div","matFormFieldLineRipple",""]],hostAttrs:[1,"mdc-line-ripple"]})}return n})(),nE=(()=>{class n{_elementRef=u(P);_ngZone=u(F);open=!1;_notch;ngAfterViewInit(){let e=this._elementRef.nativeElement,i=e.querySelector(".mdc-floating-label");i?(e.classList.add("mdc-notched-outline--upgraded"),typeof requestAnimationFrame=="function"&&(i.style.transitionDuration="0s",this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>i.style.transitionDuration="")}))):e.classList.add("mdc-notched-outline--no-label")}_setNotchWidth(e){let i=this._notch.nativeElement;!this.open||!e?i.style.width="":i.style.width=`calc(${e}px * var(--mat-mdc-form-field-floating-label-scale, 0.75) + 9px)`}_setMaxWidth(e){this._notch.nativeElement.style.setProperty("--mat-form-field-notch-max-width",`calc(100% - ${e}px)`)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["div","matFormFieldNotchedOutline",""]],viewQuery:function(i,r){if(i&1&&Te(fN,5),i&2){let o;G(o=W())&&(r._notch=o.first)}},hostAttrs:[1,"mdc-notched-outline"],hostVars:2,hostBindings:function(i,r){i&2&&j("mdc-notched-outline--notched",r.open)},inputs:{open:[0,"matFormFieldNotchedOutlineOpen","open"]},attrs:pN,ngContentSelectors:gN,decls:5,vars:0,consts:[["notch",""],[1,"mat-mdc-notch-piece","mdc-notched-outline__leading"],[1,"mat-mdc-notch-piece","mdc-notched-outline__notch"],[1,"mat-mdc-notch-piece","mdc-notched-outline__trailing"]],template:function(i,r){i&1&&(Ee(),on(0,"div",1),Ot(1,"div",2,0),ie(3),$t(),on(4,"div",3))},encapsulation:2,changeDetection:0})}return n})(),rl=(()=>{class n{value=null;stateChanges;id;placeholder;ngControl=null;focused=!1;empty=!1;shouldLabelFloat=!1;required=!1;disabled=!1;errorState=!1;controlType;autofilled;userAriaDescribedBy;disableAutomaticLabeling;describedByIds;static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n})}return n})();var ol=new v("MatFormField"),jN=new v("MAT_FORM_FIELD_DEFAULT_OPTIONS"),iE="fill",zN="auto",rE="fixed",HN="translateY(-50%)",Wn=(()=>{class n{_elementRef=u(P);_changeDetectorRef=u(Ae);_platform=u(_e);_idGenerator=u(Ie);_ngZone=u(F);_defaults=u(jN,{optional:!0});_currentDirection;_textField;_iconPrefixContainer;_textPrefixContainer;_iconSuffixContainer;_textSuffixContainer;_floatingLabel;_notchedOutline;_lineRipple;_iconPrefixContainerSignal=pa("iconPrefixContainer");_textPrefixContainerSignal=pa("textPrefixContainer");_iconSuffixContainerSignal=pa("iconSuffixContainer");_textSuffixContainerSignal=pa("textSuffixContainer");_prefixSuffixContainers=wn(()=>[this._iconPrefixContainerSignal(),this._textPrefixContainerSignal(),this._iconSuffixContainerSignal(),this._textSuffixContainerSignal()].map(e=>e?.nativeElement).filter(e=>e!==void 0));_formFieldControl;_prefixChildren;_suffixChildren;_errorChildren;_hintChildren;_labelChild=Qw(pi);get hideRequiredMarker(){return this._hideRequiredMarker}set hideRequiredMarker(e){this._hideRequiredMarker=fi(e)}_hideRequiredMarker=!1;color="primary";get floatLabel(){return this._floatLabel||this._defaults?.floatLabel||zN}set floatLabel(e){e!==this._floatLabel&&(this._floatLabel=e,this._changeDetectorRef.markForCheck())}_floatLabel;get appearance(){return this._appearanceSignal()}set appearance(e){let i=e||this._defaults?.appearance||iE;this._appearanceSignal.set(i)}_appearanceSignal=Oe(iE);get subscriptSizing(){return this._subscriptSizing||this._defaults?.subscriptSizing||rE}set subscriptSizing(e){this._subscriptSizing=e||this._defaults?.subscriptSizing||rE}_subscriptSizing=null;get hintLabel(){return this._hintLabel}set hintLabel(e){this._hintLabel=e,this._processHints()}_hintLabel="";_hasIconPrefix=!1;_hasTextPrefix=!1;_hasIconSuffix=!1;_hasTextSuffix=!1;_labelId=this._idGenerator.getId("mat-mdc-form-field-label-");_hintLabelId=this._idGenerator.getId("mat-mdc-hint-");_describedByIds;get _control(){return this._explicitFormFieldControl||this._formFieldControl}set _control(e){this._explicitFormFieldControl=e}_destroyed=new k;_isFocused=null;_explicitFormFieldControl;_previousControl=null;_previousControlValidatorFn=null;_stateChanges;_valueChanges;_describedByChanges;_outlineLabelOffsetResizeObserver=null;_animationsDisabled=Fe();constructor(){let e=this._defaults,i=u(st);e&&(e.appearance&&(this.appearance=e.appearance),this._hideRequiredMarker=!!e?.hideRequiredMarker,e.color&&(this.color=e.color)),Fi(()=>this._currentDirection=i.valueSignal()),this._syncOutlineLabelOffset()}ngAfterViewInit(){this._updateFocusState(),this._animationsDisabled||this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-form-field-animations-enabled")},300)}),this._changeDetectorRef.detectChanges()}ngAfterContentInit(){this._assertFormFieldControl(),this._initializeSubscript(),this._initializePrefixAndSuffix()}ngAfterContentChecked(){this._assertFormFieldControl(),this._control!==this._previousControl&&(this._initializeControl(this._previousControl),this._control.ngControl&&this._control.ngControl.control&&(this._previousControlValidatorFn=this._control.ngControl.control.validator),this._previousControl=this._control),this._control.ngControl&&this._control.ngControl.control&&this._control.ngControl.control.validator!==this._previousControlValidatorFn&&this._changeDetectorRef.markForCheck()}ngOnDestroy(){this._outlineLabelOffsetResizeObserver?.disconnect(),this._stateChanges?.unsubscribe(),this._valueChanges?.unsubscribe(),this._describedByChanges?.unsubscribe(),this._destroyed.next(),this._destroyed.complete()}getLabelId=wn(()=>this._hasFloatingLabel()?this._labelId:null);getConnectedOverlayOrigin(){return this._textField||this._elementRef}_animateAndLockLabel(){this._hasFloatingLabel()&&(this.floatLabel="always")}_initializeControl(e){let i=this._control,r="mat-mdc-form-field-type-";e&&this._elementRef.nativeElement.classList.remove(r+e.controlType),i.controlType&&this._elementRef.nativeElement.classList.add(r+i.controlType),this._stateChanges?.unsubscribe(),this._stateChanges=i.stateChanges.subscribe(()=>{this._updateFocusState(),this._changeDetectorRef.markForCheck()}),this._describedByChanges?.unsubscribe(),this._describedByChanges=i.stateChanges.pipe(jt([void 0,void 0]),ye(()=>[i.errorState,i.userAriaDescribedBy]),fc(),ze(([[o,s],[a,l]])=>o!==a||s!==l)).subscribe(()=>this._syncDescribedByIds()),this._valueChanges?.unsubscribe(),i.ngControl&&i.ngControl.valueChanges&&(this._valueChanges=i.ngControl.valueChanges.pipe(Ce(this._destroyed)).subscribe(()=>this._changeDetectorRef.markForCheck()))}_checkPrefixAndSuffixTypes(){this._hasIconPrefix=!!this._prefixChildren.find(e=>!e._isText),this._hasTextPrefix=!!this._prefixChildren.find(e=>e._isText),this._hasIconSuffix=!!this._suffixChildren.find(e=>!e._isText),this._hasTextSuffix=!!this._suffixChildren.find(e=>e._isText)}_initializePrefixAndSuffix(){this._checkPrefixAndSuffixTypes(),Zt(this._prefixChildren.changes,this._suffixChildren.changes).subscribe(()=>{this._checkPrefixAndSuffixTypes(),this._changeDetectorRef.markForCheck()})}_initializeSubscript(){this._hintChildren.changes.subscribe(()=>{this._processHints(),this._changeDetectorRef.markForCheck()}),this._errorChildren.changes.subscribe(()=>{this._syncDescribedByIds(),this._changeDetectorRef.markForCheck()}),this._validateHints(),this._syncDescribedByIds()}_assertFormFieldControl(){this._control}_updateFocusState(){let e=this._control.focused;e&&!this._isFocused?(this._isFocused=!0,this._lineRipple?.activate()):!e&&(this._isFocused||this._isFocused===null)&&(this._isFocused=!1,this._lineRipple?.deactivate()),this._elementRef.nativeElement.classList.toggle("mat-focused",e),this._textField?.nativeElement.classList.toggle("mdc-text-field--focused",e)}_syncOutlineLabelOffset(){eC({earlyRead:()=>{if(this._appearanceSignal()!=="outline")return this._outlineLabelOffsetResizeObserver?.disconnect(),null;if(globalThis.ResizeObserver){this._outlineLabelOffsetResizeObserver||=new globalThis.ResizeObserver(()=>{this._writeOutlinedLabelStyles(this._getOutlinedLabelOffset())});for(let e of this._prefixSuffixContainers())this._outlineLabelOffsetResizeObserver.observe(e,{box:"border-box"})}return this._getOutlinedLabelOffset()},write:e=>this._writeOutlinedLabelStyles(e())})}_shouldAlwaysFloat(){return this.floatLabel==="always"}_hasOutline(){return this.appearance==="outline"}_forceDisplayInfixLabel(){return!this._platform.isBrowser&&this._prefixChildren.length&&!this._shouldLabelFloat()}_hasFloatingLabel=wn(()=>!!this._labelChild());_shouldLabelFloat(){return this._hasFloatingLabel()?this._control.shouldLabelFloat||this._shouldAlwaysFloat():!1}_shouldForward(e){let i=this._control?this._control.ngControl:null;return i&&i[e]}_getSubscriptMessageType(){return this._errorChildren&&this._errorChildren.length>0&&this._control.errorState?"error":"hint"}_handleLabelResized(){this._refreshOutlineNotchWidth()}_refreshOutlineNotchWidth(){!this._hasOutline()||!this._floatingLabel||!this._shouldLabelFloat()?this._notchedOutline?._setNotchWidth(0):this._notchedOutline?._setNotchWidth(this._floatingLabel.getWidth())}_processHints(){this._validateHints(),this._syncDescribedByIds()}_validateHints(){this._hintChildren}_syncDescribedByIds(){if(this._control){let e=[];if(this._control.userAriaDescribedBy&&typeof this._control.userAriaDescribedBy=="string"&&e.push(...this._control.userAriaDescribedBy.split(" ")),this._getSubscriptMessageType()==="hint"){let o=this._hintChildren?this._hintChildren.find(a=>a.align==="start"):null,s=this._hintChildren?this._hintChildren.find(a=>a.align==="end"):null;o?e.push(o.id):this._hintLabel&&e.push(this._hintLabelId),s&&e.push(s.id)}else this._errorChildren&&e.push(...this._errorChildren.map(o=>o.id));let i=this._control.describedByIds,r;if(i){let o=this._describedByIds||e;r=e.concat(i.filter(s=>s&&!o.includes(s)))}else r=e;this._control.setDescribedByIds(r),this._describedByIds=e}}_getOutlinedLabelOffset(){if(!this._hasOutline()||!this._floatingLabel)return null;if(!this._iconPrefixContainer&&!this._textPrefixContainer)return["",null];if(!this._isAttachedToDom())return null;let e=this._iconPrefixContainer?.nativeElement,i=this._textPrefixContainer?.nativeElement,r=this._iconSuffixContainer?.nativeElement,o=this._textSuffixContainer?.nativeElement,s=e?.getBoundingClientRect().width??0,a=i?.getBoundingClientRect().width??0,l=r?.getBoundingClientRect().width??0,c=o?.getBoundingClientRect().width??0,d=this._currentDirection==="rtl"?"-1":"1",m=`${s+a}px`,h=`calc(${d} * (${m} + var(--mat-mdc-form-field-label-offset-x, 0px)))`,p=`var(--mat-mdc-form-field-label-transform, ${HN} translateX(${h}))`,b=s+a+l+c;return[p,b]}_writeOutlinedLabelStyles(e){if(e!==null){let[i,r]=e;this._floatingLabel&&(this._floatingLabel.element.style.transform=i),r!==null&&this._notchedOutline?._setMaxWidth(r)}}_isAttachedToDom(){let e=this._elementRef.nativeElement;if(e.getRootNode){let i=e.getRootNode();return i&&i!==e}return document.documentElement.contains(e)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-form-field"]],contentQueries:function(i,r,o){if(i&1&&(Td(o,r._labelChild,pi,5),kt(o,rl,5)(o,LN,5)(o,BN,5)(o,oE,5)(o,E_,5)),i&2){Rd();let s;G(s=W())&&(r._formFieldControl=s.first),G(s=W())&&(r._prefixChildren=s),G(s=W())&&(r._suffixChildren=s),G(s=W())&&(r._errorChildren=s),G(s=W())&&(r._hintChildren=s)}},viewQuery:function(i,r){if(i&1&&(Ad(r._iconPrefixContainerSignal,Kx,5)(r._textPrefixContainerSignal,Yx,5)(r._iconSuffixContainerSignal,Zx,5)(r._textSuffixContainerSignal,Xx,5),Te(_N,5)(Kx,5)(Yx,5)(Zx,5)(Xx,5)(Jx,5)(nE,5)(tE,5)),i&2){Rd(4);let o;G(o=W())&&(r._textField=o.first),G(o=W())&&(r._iconPrefixContainer=o.first),G(o=W())&&(r._textPrefixContainer=o.first),G(o=W())&&(r._iconSuffixContainer=o.first),G(o=W())&&(r._textSuffixContainer=o.first),G(o=W())&&(r._floatingLabel=o.first),G(o=W())&&(r._notchedOutline=o.first),G(o=W())&&(r._lineRipple=o.first)}},hostAttrs:[1,"mat-mdc-form-field"],hostVars:38,hostBindings:function(i,r){i&2&&j("mat-mdc-form-field-label-always-float",r._shouldAlwaysFloat())("mat-mdc-form-field-has-icon-prefix",r._hasIconPrefix)("mat-mdc-form-field-has-icon-suffix",r._hasIconSuffix)("mat-form-field-invalid",r._control.errorState)("mat-form-field-disabled",r._control.disabled)("mat-form-field-autofilled",r._control.autofilled)("mat-form-field-appearance-fill",r.appearance=="fill")("mat-form-field-appearance-outline",r.appearance=="outline")("mat-form-field-hide-placeholder",r._hasFloatingLabel()&&!r._shouldLabelFloat())("mat-primary",r.color!=="accent"&&r.color!=="warn")("mat-accent",r.color==="accent")("mat-warn",r.color==="warn")("ng-untouched",r._shouldForward("untouched"))("ng-touched",r._shouldForward("touched"))("ng-pristine",r._shouldForward("pristine"))("ng-dirty",r._shouldForward("dirty"))("ng-valid",r._shouldForward("valid"))("ng-invalid",r._shouldForward("invalid"))("ng-pending",r._shouldForward("pending"))},inputs:{hideRequiredMarker:"hideRequiredMarker",color:"color",floatLabel:"floatLabel",appearance:"appearance",subscriptSizing:"subscriptSizing",hintLabel:"hintLabel"},exportAs:["matFormField"],features:[ve([{provide:ol,useExisting:n},{provide:sE,useExisting:n}])],ngContentSelectors:bN,decls:18,vars:21,consts:[["labelTemplate",""],["textField",""],["iconPrefixContainer",""],["textPrefixContainer",""],["textSuffixContainer",""],["iconSuffixContainer",""],[1,"mat-mdc-text-field-wrapper","mdc-text-field",3,"click"],[1,"mat-mdc-form-field-focus-overlay"],[1,"mat-mdc-form-field-flex"],["matFormFieldNotchedOutline","",3,"matFormFieldNotchedOutlineOpen"],[1,"mat-mdc-form-field-icon-prefix"],[1,"mat-mdc-form-field-text-prefix"],[1,"mat-mdc-form-field-infix"],[3,"ngTemplateOutlet"],[1,"mat-mdc-form-field-text-suffix"],[1,"mat-mdc-form-field-icon-suffix"],["matFormFieldLineRipple",""],["aria-atomic","true","aria-live","polite",1,"mat-mdc-form-field-subscript-wrapper","mat-mdc-form-field-bottom-align"],[1,"mat-mdc-form-field-error-wrapper"],[1,"mat-mdc-form-field-hint-wrapper"],["matFormFieldFloatingLabel","",3,"floating","monitorResize","id"],["aria-hidden","true",1,"mat-mdc-form-field-required-marker","mdc-floating-label--required"],[3,"id"],[1,"mat-mdc-form-field-hint-spacer"]],template:function(i,r){if(i&1&&(Ee(vN),fe(0,CN,1,1,"ng-template",null,0,Vo),_(2,"div",6,1),V("click",function(s){return r._control.onContainerClick(s)}),ce(4,DN,1,0,"div",7),_(5,"div",8),ce(6,kN,2,2,"div",9),ce(7,SN,3,0,"div",10),ce(8,IN,3,0,"div",11),_(9,"div",12),ce(10,TN,1,1,null,13),ie(11),g(),ce(12,AN,3,0,"div",14),ce(13,RN,3,0,"div",15),g(),ce(14,ON,1,0,"div",16),g(),_(15,"div",17),ce(16,NN,2,0,"div",18)(17,PN,5,1,"div",19),g()),i&2){let o;y(2),j("mdc-text-field--filled",!r._hasOutline())("mdc-text-field--outlined",r._hasOutline())("mdc-text-field--no-label",!r._hasFloatingLabel())("mdc-text-field--disabled",r._control.disabled)("mdc-text-field--invalid",r._control.errorState),y(2),de(!r._hasOutline()&&!r._control.disabled?4:-1),y(2),de(r._hasOutline()?6:-1),y(),de(r._hasIconPrefix?7:-1),y(),de(r._hasTextPrefix?8:-1),y(2),de(!r._hasOutline()||r._forceDisplayInfixLabel()?10:-1),y(2),de(r._hasTextSuffix?12:-1),y(),de(r._hasIconSuffix?13:-1),y(),de(r._hasOutline()?-1:14),y(),j("mat-mdc-form-field-subscript-dynamic-size",r.subscriptSizing==="dynamic");let s=r._getSubscriptMessageType();y(),de((o=s)==="error"?16:o==="hint"?17:-1)}},dependencies:[Jx,nE,va,tE,E_],styles:[`.mdc-text-field {
  display: inline-flex;
  align-items: baseline;
  padding: 0 16px;
  position: relative;
  box-sizing: border-box;
  overflow: hidden;
  will-change: opacity, transform, color;
  border-top-left-radius: 4px;
  border-top-right-radius: 4px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
}

.mdc-text-field__input {
  width: 100%;
  min-width: 0;
  border: none;
  border-radius: 0;
  background: none;
  padding: 0;
  -moz-appearance: none;
  -webkit-appearance: none;
  height: 28px;
}
.mdc-text-field__input::-webkit-calendar-picker-indicator, .mdc-text-field__input::-webkit-search-cancel-button {
  display: none;
}
.mdc-text-field__input::-ms-clear {
  display: none;
}
.mdc-text-field__input:focus {
  outline: none;
}
.mdc-text-field__input:invalid {
  box-shadow: none;
}
.mdc-text-field__input::placeholder {
  opacity: 0;
}
.mdc-text-field__input::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field__input::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field__input:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mdc-text-field--focused .mdc-text-field__input::placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  opacity: 1;
}
.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  opacity: 1;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-moz-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive::-webkit-input-placeholder {
  opacity: 0;
}
.mdc-text-field--disabled:not(.mdc-text-field--no-label) .mdc-text-field__input.mat-mdc-input-disabled-interactive:-ms-input-placeholder {
  opacity: 0;
}
.mdc-text-field--outlined .mdc-text-field__input, .mdc-text-field--filled.mdc-text-field--no-label .mdc-text-field__input {
  height: 100%;
}
.mdc-text-field--outlined .mdc-text-field__input {
  display: flex;
  border: none !important;
  background-color: transparent;
}
.mdc-text-field--disabled .mdc-text-field__input {
  pointer-events: auto;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-filled-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-filled-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-filled-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input {
  color: var(--mat-form-field-outlined-input-text-color, var(--mat-sys-on-surface));
  caret-color: var(--mat-form-field-outlined-caret-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-outlined-input-text-placeholder-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-filled-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--outlined.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-text-field__input {
  caret-color: var(--mat-form-field-outlined-error-caret-color, var(--mat-sys-error));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-filled-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-text-field__input {
  color: var(--mat-form-field-outlined-disabled-input-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-text-field__input {
    background-color: Window;
  }
}

.mdc-text-field--filled {
  height: 56px;
  border-bottom-right-radius: 0;
  border-bottom-left-radius: 0;
  border-top-left-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
  border-top-right-radius: var(--mat-form-field-filled-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) {
  background-color: var(--mat-form-field-filled-container-color, var(--mat-sys-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled {
  background-color: var(--mat-form-field-filled-disabled-container-color, color-mix(in srgb, var(--mat-sys-on-surface) 4%, transparent));
}

.mdc-text-field--outlined {
  height: 56px;
  overflow: visible;
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
}
[dir=rtl] .mdc-text-field--outlined {
  padding-right: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)) + 4px);
  padding-left: max(16px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}

.mdc-floating-label {
  position: absolute;
  left: 0;
  transform-origin: left top;
  line-height: 1.15rem;
  text-align: left;
  text-overflow: ellipsis;
  white-space: nowrap;
  cursor: text;
  overflow: hidden;
  will-change: transform;
}
[dir=rtl] .mdc-floating-label {
  right: 0;
  left: auto;
  transform-origin: right top;
  text-align: right;
}
.mdc-text-field .mdc-floating-label {
  top: 50%;
  transform: translateY(-50%);
  pointer-events: none;
}
.mdc-notched-outline .mdc-floating-label {
  display: inline-block;
  position: relative;
  max-width: 100%;
}
.mdc-text-field--outlined .mdc-floating-label {
  left: 4px;
  right: auto;
}
[dir=rtl] .mdc-text-field--outlined .mdc-floating-label {
  left: auto;
  right: 4px;
}
.mdc-text-field--filled .mdc-floating-label {
  left: 16px;
  right: auto;
}
[dir=rtl] .mdc-text-field--filled .mdc-floating-label {
  left: auto;
  right: 16px;
}
.mdc-text-field--disabled .mdc-floating-label {
  cursor: default;
}
@media (forced-colors: active) {
  .mdc-text-field--disabled .mdc-floating-label {
    z-index: 1;
  }
}
.mdc-text-field--filled.mdc-text-field--no-label .mdc-floating-label {
  display: none;
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-filled-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-hover-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-filled-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-filled-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-filled-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-filled-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--filled .mdc-floating-label {
  font-family: var(--mat-form-field-filled-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-filled-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-filled-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-filled-label-text-tracking, var(--mat-sys-body-large-tracking));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mdc-floating-label {
  color: var(--mat-form-field-outlined-label-text-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-focus-label-text-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-hover-label-text-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mdc-floating-label {
  color: var(--mat-form-field-outlined-disabled-label-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-focus-label-text-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--disabled):hover .mdc-floating-label {
  color: var(--mat-form-field-outlined-error-hover-label-text-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined .mdc-floating-label {
  font-family: var(--mat-form-field-outlined-label-text-font, var(--mat-sys-body-large-font));
  font-size: var(--mat-form-field-outlined-label-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-form-field-outlined-label-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-form-field-outlined-label-text-tracking, var(--mat-sys-body-large-tracking));
}

.mdc-floating-label--float-above {
  cursor: auto;
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--filled .mdc-floating-label--float-above {
  transform: translateY(-106%) scale(0.75);
}
.mdc-text-field--outlined .mdc-floating-label--float-above {
  transform: translateY(-37.25px) scale(1);
  font-size: 0.75rem;
}
.mdc-notched-outline .mdc-floating-label--float-above {
  text-overflow: clip;
}
.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: 133.3333333333%;
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  transform: translateY(-34.75px) scale(0.75);
}
.mdc-text-field--outlined.mdc-notched-outline--upgraded .mdc-floating-label--float-above, .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: 1rem;
}

.mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 1px;
  margin-right: 0;
  content: "*";
}
[dir=rtl] .mdc-floating-label--required:not(.mdc-floating-label--hide-required-marker)::after {
  margin-left: 0;
  margin-right: 1px;
}

.mdc-notched-outline {
  display: flex;
  position: absolute;
  top: 0;
  right: 0;
  left: 0;
  box-sizing: border-box;
  width: 100%;
  max-width: 100%;
  height: 100%;
  text-align: left;
  pointer-events: none;
}
[dir=rtl] .mdc-notched-outline {
  text-align: right;
}
.mdc-text-field--outlined .mdc-notched-outline {
  z-index: 1;
}

.mat-mdc-notch-piece {
  box-sizing: border-box;
  height: 100%;
  pointer-events: none;
  border: none;
  border-top: 1px solid;
  border-bottom: 1px solid;
}
.mdc-text-field--focused .mat-mdc-notch-piece {
  border-width: 2px;
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled) .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-outline-color, var(--mat-sys-outline));
  border-width: var(--mat-form-field-outlined-outline-width, 1px);
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-hover-outline-color, var(--mat-sys-on-surface));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-focus-outline-color, var(--mat-sys-primary));
}
.mdc-text-field--outlined.mdc-text-field--disabled .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-disabled-outline-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-notched-outline .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-hover-outline-color, var(--mat-sys-on-error-container));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--invalid.mdc-text-field--focused .mat-mdc-notch-piece {
  border-color: var(--mat-form-field-outlined-error-focus-outline-color, var(--mat-sys-error));
}
.mdc-text-field--outlined:not(.mdc-text-field--disabled).mdc-text-field--focused .mdc-notched-outline .mat-mdc-notch-piece {
  border-width: var(--mat-form-field-outlined-focus-outline-width, 2px);
}

.mdc-notched-outline__leading {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__leading {
  width: max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small)));
}
[dir=rtl] .mdc-notched-outline__leading {
  border-left: none;
  border-right: 1px solid;
  border-bottom-left-radius: 0;
  border-top-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__trailing {
  flex-grow: 1;
  border-left: none;
  border-right: 1px solid;
  border-top-left-radius: 0;
  border-bottom-left-radius: 0;
  border-top-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-right-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}
[dir=rtl] .mdc-notched-outline__trailing {
  border-left: 1px solid;
  border-right: none;
  border-top-right-radius: 0;
  border-bottom-right-radius: 0;
  border-top-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
  border-bottom-left-radius: var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small));
}

.mdc-notched-outline__notch {
  flex: 0 0 auto;
  width: auto;
}
.mdc-text-field--outlined .mdc-notched-outline .mdc-notched-outline__notch {
  max-width: min(var(--mat-form-field-notch-max-width, 100%), calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  max-width: min(100%, calc(100% - max(12px, var(--mat-form-field-outlined-container-shape, var(--mat-sys-corner-extra-small))) * 2));
}
.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 1px;
}
.mdc-text-field--focused.mdc-text-field--outlined .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-top: 2px;
}
.mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 0;
  padding-right: 8px;
  border-top: none;
}
[dir=rtl] .mdc-notched-outline--notched .mdc-notched-outline__notch {
  padding-left: 8px;
  padding-right: 0;
}
.mdc-notched-outline--no-label .mdc-notched-outline__notch {
  display: none;
}

.mdc-line-ripple::before, .mdc-line-ripple::after {
  position: absolute;
  bottom: 0;
  left: 0;
  width: 100%;
  border-bottom-style: solid;
  content: "";
}
.mdc-line-ripple::before {
  z-index: 1;
  border-bottom-width: var(--mat-form-field-filled-active-indicator-height, 1px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-active-indicator-color, var(--mat-sys-on-surface-variant));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled):not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-hover-active-indicator-color, var(--mat-sys-on-surface));
}
.mdc-text-field--filled.mdc-text-field--disabled .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-disabled-active-indicator-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-active-indicator-color, var(--mat-sys-error));
}
.mdc-text-field--filled:not(.mdc-text-field--disabled).mdc-text-field--invalid:not(.mdc-text-field--focused):hover .mdc-line-ripple::before {
  border-bottom-color: var(--mat-form-field-filled-error-hover-active-indicator-color, var(--mat-sys-on-error-container));
}
.mdc-line-ripple::after {
  transform: scaleX(0);
  opacity: 0;
  z-index: 2;
}
.mdc-text-field--filled .mdc-line-ripple::after {
  border-bottom-width: var(--mat-form-field-filled-focus-active-indicator-height, 2px);
}
.mdc-text-field--filled:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-focus-active-indicator-color, var(--mat-sys-primary));
}
.mdc-text-field--filled.mdc-text-field--invalid:not(.mdc-text-field--disabled) .mdc-line-ripple::after {
  border-bottom-color: var(--mat-form-field-filled-error-focus-active-indicator-color, var(--mat-sys-error));
}

.mdc-line-ripple--active::after {
  transform: scaleX(1);
  opacity: 1;
}

.mdc-line-ripple--deactivating::after {
  opacity: 0;
}

.mdc-text-field--disabled {
  pointer-events: none;
}

.mat-mdc-form-field-textarea-control {
  vertical-align: middle;
  resize: vertical;
  box-sizing: border-box;
  height: auto;
  margin: 0;
  padding: 0;
  border: none;
  overflow: auto;
}

.mat-mdc-form-field-input-control.mat-mdc-form-field-input-control {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font: inherit;
  letter-spacing: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  border: none;
}

.mat-mdc-form-field .mat-mdc-floating-label.mdc-floating-label {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  line-height: normal;
  pointer-events: all;
  will-change: auto;
}

.mat-mdc-form-field:not(.mat-form-field-disabled) .mat-mdc-floating-label.mdc-floating-label {
  cursor: inherit;
}

.mdc-text-field--no-label:not(.mdc-text-field--textarea) .mat-mdc-form-field-input-control.mdc-text-field__input,
.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control {
  height: auto;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-input-control.mdc-text-field__input[type=color] {
  height: 23px;
}

.mat-mdc-text-field-wrapper {
  height: auto;
  flex: auto;
  will-change: auto;
}

.mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-left: 0;
  --mat-mdc-form-field-label-offset-x: -16px;
}

.mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

[dir=rtl] .mat-mdc-text-field-wrapper {
  padding-left: 16px;
  padding-right: 16px;
}
[dir=rtl] .mat-mdc-form-field-has-icon-suffix .mat-mdc-text-field-wrapper {
  padding-left: 0;
}
[dir=rtl] .mat-mdc-form-field-has-icon-prefix .mat-mdc-text-field-wrapper {
  padding-right: 0;
}

.mat-form-field-disabled .mdc-text-field__input::placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-moz-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input::-webkit-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-disabled .mdc-text-field__input:-ms-input-placeholder {
  color: var(--mat-form-field-disabled-input-text-placeholder-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-label-always-float .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
  opacity: 1;
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-infix .mat-mdc-floating-label {
  left: auto;
  right: auto;
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-text-field__input {
  display: inline-block;
}

.mat-mdc-form-field .mat-mdc-text-field-wrapper.mdc-text-field .mdc-notched-outline__notch {
  padding-top: 0;
}

.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: 1px solid transparent;
}

[dir=rtl] .mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field.mat-mdc-form-field .mdc-notched-outline__notch {
  border-left: none;
  border-right: 1px solid transparent;
}

.mat-mdc-form-field-infix {
  min-height: var(--mat-form-field-container-height, 56px);
  padding-top: var(--mat-form-field-filled-with-label-container-padding-top, 24px);
  padding-bottom: var(--mat-form-field-filled-with-label-container-padding-bottom, 8px);
}
.mdc-text-field--outlined .mat-mdc-form-field-infix, .mdc-text-field--no-label .mat-mdc-form-field-infix {
  padding-top: var(--mat-form-field-container-vertical-padding, 16px);
  padding-bottom: var(--mat-form-field-container-vertical-padding, 16px);
}

.mat-mdc-text-field-wrapper .mat-mdc-form-field-flex .mat-mdc-floating-label {
  top: calc(var(--mat-form-field-container-height, 56px) / 2);
}

.mdc-text-field--filled .mat-mdc-floating-label {
  display: var(--mat-form-field-filled-label-display, block);
}

.mat-mdc-text-field-wrapper.mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  --mat-mdc-form-field-label-transform: translateY(calc(calc(6.75px + var(--mat-form-field-container-height, 56px) / 2) * -1))
    scale(var(--mat-mdc-form-field-floating-label-scale, 0.75));
  transform: var(--mat-mdc-form-field-label-transform);
}

@keyframes _mat-form-field-subscript-animation {
  from {
    opacity: 0;
    transform: translateY(-5px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}
.mat-mdc-form-field-subscript-wrapper {
  box-sizing: border-box;
  width: 100%;
  position: relative;
}

.mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-error-wrapper {
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  padding: 0 16px;
  opacity: 1;
  transform: translateY(0);
  animation: _mat-form-field-subscript-animation 0ms cubic-bezier(0.55, 0, 0.55, 0.2);
}

.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field-subscript-dynamic-size .mat-mdc-form-field-error-wrapper {
  position: static;
}

.mat-mdc-form-field-bottom-align::before {
  content: "";
  display: inline-block;
  height: 16px;
}

.mat-mdc-form-field-bottom-align.mat-mdc-form-field-subscript-dynamic-size::before {
  content: unset;
}

.mat-mdc-form-field-hint-end {
  order: 1;
}

.mat-mdc-form-field-hint-wrapper {
  display: flex;
}

.mat-mdc-form-field-hint-spacer {
  flex: 1 0 1em;
}

.mat-mdc-form-field-error {
  display: block;
  color: var(--mat-form-field-error-text-color, var(--mat-sys-error));
}

.mat-mdc-form-field-subscript-wrapper,
.mat-mdc-form-field-bottom-align::before {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-subscript-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-form-field-subscript-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-form-field-subscript-text-size, var(--mat-sys-body-small-size));
  letter-spacing: var(--mat-form-field-subscript-text-tracking, var(--mat-sys-body-small-tracking));
  font-weight: var(--mat-form-field-subscript-text-weight, var(--mat-sys-body-small-weight));
}

.mat-mdc-form-field-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  opacity: 0;
  pointer-events: none;
  background-color: var(--mat-form-field-state-layer-color, var(--mat-sys-on-surface));
}
.mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-mdc-form-field.mat-focused .mat-mdc-form-field-focus-overlay {
  opacity: var(--mat-form-field-focus-state-layer-opacity, 0);
}

select.mat-mdc-form-field-input-control {
  -moz-appearance: none;
  -webkit-appearance: none;
  background-color: transparent;
  display: inline-flex;
  box-sizing: border-box;
}
select.mat-mdc-form-field-input-control:not(:disabled) {
  cursor: pointer;
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option {
  color: var(--mat-form-field-select-option-text-color, var(--mat-sys-neutral10));
}
select.mat-mdc-form-field-input-control:not(.mat-mdc-native-select-inline) option:disabled {
  color: var(--mat-form-field-select-disabled-option-text-color, color-mix(in srgb, var(--mat-sys-neutral10) 38%, transparent));
}

.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  content: "";
  width: 0;
  height: 0;
  border-left: 5px solid transparent;
  border-right: 5px solid transparent;
  border-top: 5px solid;
  position: absolute;
  right: 0;
  top: 50%;
  margin-top: -2.5px;
  pointer-events: none;
  color: var(--mat-form-field-enabled-select-arrow-color, var(--mat-sys-on-surface-variant));
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-infix::after {
  right: auto;
  left: 0;
}
.mat-mdc-form-field-type-mat-native-select.mat-focused .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-focus-select-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field-type-mat-native-select.mat-form-field-disabled .mat-mdc-form-field-infix::after {
  color: var(--mat-form-field-disabled-select-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 15px;
}
[dir=rtl] .mat-mdc-form-field-type-mat-native-select .mat-mdc-form-field-input-control {
  padding-right: 0;
  padding-left: 15px;
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill .mat-mdc-text-field-wrapper {
    outline: solid 1px;
  }
}
@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-form-field-disabled .mat-mdc-text-field-wrapper {
    outline-color: GrayText;
  }
}

@media (forced-colors: active) {
  .mat-form-field-appearance-fill.mat-focused .mat-mdc-text-field-wrapper {
    outline: dashed 3px;
  }
}

@media (forced-colors: active) {
  .mat-mdc-form-field.mat-focused .mdc-notched-outline {
    border: dashed 3px;
  }
}

.mat-mdc-form-field-input-control[type=date], .mat-mdc-form-field-input-control[type=datetime], .mat-mdc-form-field-input-control[type=datetime-local], .mat-mdc-form-field-input-control[type=month], .mat-mdc-form-field-input-control[type=week], .mat-mdc-form-field-input-control[type=time] {
  line-height: 1;
}
.mat-mdc-form-field-input-control::-webkit-datetime-edit {
  line-height: 1;
  padding: 0;
  margin-bottom: -2px;
}

.mat-mdc-form-field {
  --mat-mdc-form-field-floating-label-scale: 0.75;
  display: inline-flex;
  flex-direction: column;
  min-width: 0;
  text-align: left;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-form-field-container-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-form-field-container-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-form-field-container-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-form-field-container-text-tracking, var(--mat-sys-body-large-tracking));
  font-weight: var(--mat-form-field-container-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-floating-label--float-above {
  font-size: calc(var(--mat-form-field-outlined-label-text-populated-size) * var(--mat-mdc-form-field-floating-label-scale));
}
.mat-mdc-form-field .mdc-text-field--outlined .mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  font-size: var(--mat-form-field-outlined-label-text-populated-size);
}
[dir=rtl] .mat-mdc-form-field {
  text-align: right;
}

.mat-mdc-form-field-flex {
  display: inline-flex;
  align-items: baseline;
  box-sizing: border-box;
  width: 100%;
}

.mat-mdc-text-field-wrapper {
  width: 100%;
  z-index: 0;
}

.mat-mdc-form-field-icon-prefix,
.mat-mdc-form-field-icon-suffix {
  align-self: center;
  line-height: 0;
  pointer-events: auto;
  position: relative;
  z-index: 1;
}
.mat-mdc-form-field-icon-prefix > .mat-icon,
.mat-mdc-form-field-icon-suffix > .mat-icon {
  padding: 0 12px;
  box-sizing: content-box;
}

.mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-leading-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-prefix {
  color: var(--mat-form-field-disabled-leading-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-trailing-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-form-field-disabled .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-disabled-trailing-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-form-field-invalid .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-trailing-icon-color, var(--mat-sys-error));
}
.mat-form-field-invalid:not(.mat-focused):not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper:hover .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-hover-trailing-icon-color, var(--mat-sys-on-error-container));
}
.mat-form-field-invalid.mat-focused .mat-mdc-text-field-wrapper .mat-mdc-form-field-icon-suffix {
  color: var(--mat-form-field-error-focus-trailing-icon-color, var(--mat-sys-error));
}

.mat-mdc-form-field-icon-prefix,
[dir=rtl] .mat-mdc-form-field-icon-suffix {
  padding: 0 4px 0 0;
}

.mat-mdc-form-field-icon-suffix,
[dir=rtl] .mat-mdc-form-field-icon-prefix {
  padding: 0 0 0 4px;
}

.mat-mdc-form-field-subscript-wrapper .mat-icon,
.mat-mdc-form-field label .mat-icon {
  width: 1em;
  height: 1em;
  font-size: inherit;
}

.mat-mdc-form-field-infix {
  flex: auto;
  min-width: 0;
  width: 180px;
  position: relative;
  box-sizing: border-box;
}
.mat-mdc-form-field-infix:has(textarea[cols]) {
  width: auto;
}

.mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: -1px;
  -webkit-clip-path: inset(-9em -999em -9em 1px);
  clip-path: inset(-9em -999em -9em 1px);
}
[dir=rtl] .mat-mdc-form-field .mdc-notched-outline__notch {
  margin-left: 0;
  margin-right: -1px;
  -webkit-clip-path: inset(-9em 1px -9em -999em);
  clip-path: inset(-9em 1px -9em -999em);
}

.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-floating-label {
  transition: transform 150ms cubic-bezier(0.4, 0, 0.2, 1), color 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input {
  transition: opacity 150ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-moz-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input::-webkit-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field__input:-ms-input-placeholder {
  transition: opacity 67ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-moz-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-moz-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input::-webkit-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input::-webkit-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--no-label .mdc-text-field__input:-ms-input-placeholder, .mat-mdc-form-field.mat-form-field-animations-enabled.mdc-text-field--focused .mdc-text-field__input:-ms-input-placeholder {
  transition-delay: 40ms;
  transition-duration: 110ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-text-field--filled:not(.mdc-ripple-upgraded):focus .mdc-text-field__ripple::before {
  transition-duration: 75ms;
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mdc-line-ripple::after {
  transition: transform 180ms cubic-bezier(0.4, 0, 0.2, 1), opacity 180ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-hint-wrapper,
.mat-mdc-form-field.mat-form-field-animations-enabled .mat-mdc-form-field-error-wrapper {
  animation-duration: 300ms;
}

.mdc-notched-outline .mdc-floating-label {
  max-width: calc(100% + 1px);
}

.mdc-notched-outline--upgraded .mdc-floating-label--float-above {
  max-width: calc(133.3333333333% + 1px);
}
`],encapsulation:2,changeDetection:0})}return n})();var Yi=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[ss,Wn,oe]})}return n})();var UN=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["ng-component"]],hostAttrs:["cdk-text-field-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`textarea.cdk-textarea-autosize {
  resize: none;
}

textarea.cdk-textarea-autosize-measuring {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: auto !important;
  overflow: hidden !important;
}

textarea.cdk-textarea-autosize-measuring-firefox {
  padding: 2px 0 !important;
  box-sizing: content-box !important;
  height: 0 !important;
}

@keyframes cdk-text-field-autofill-start { /*!*/ }
@keyframes cdk-text-field-autofill-end { /*!*/ }
.cdk-text-field-autofill-monitored:-webkit-autofill {
  animation: cdk-text-field-autofill-start 0s 1ms;
}

.cdk-text-field-autofill-monitored:not(:-webkit-autofill) {
  animation: cdk-text-field-autofill-end 0s 1ms;
}
`],encapsulation:2,changeDetection:0})}return n})(),$N={passive:!0},aE=(()=>{class n{_platform=u(_e);_ngZone=u(F);_renderer=u(qe).createRenderer(null,null);_styleLoader=u(it);_monitoredElements=new Map;constructor(){}monitor(e){if(!this._platform.isBrowser)return ur;this._styleLoader.load(UN);let i=Wt(e),r=this._monitoredElements.get(i);if(r)return r.subject;let o=new k,s="cdk-text-field-autofilled",a=c=>{c.animationName==="cdk-text-field-autofill-start"&&!i.classList.contains(s)?(i.classList.add(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!0}))):c.animationName==="cdk-text-field-autofill-end"&&i.classList.contains(s)&&(i.classList.remove(s),this._ngZone.run(()=>o.next({target:c.target,isAutofilled:!1})))},l=this._ngZone.runOutsideAngular(()=>(i.classList.add("cdk-text-field-autofill-monitored"),this._renderer.listen(i,"animationstart",a,$N)));return this._monitoredElements.set(i,{subject:o,unlisten:l}),o}stopMonitoring(e){let i=Wt(e),r=this._monitoredElements.get(i);r&&(r.unlisten(),r.subject.complete(),i.classList.remove("cdk-text-field-autofill-monitored"),i.classList.remove("cdk-text-field-autofilled"),this._monitoredElements.delete(i))}ngOnDestroy(){this._monitoredElements.forEach((e,i)=>this.stopMonitoring(i))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var lE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})();var cE=new v("MAT_INPUT_VALUE_ACCESSOR");var cs=(()=>{class n{isErrorState(e,i){return!!(e&&e.invalid&&(e.touched||i&&i.submitted))}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var ds=class{_defaultMatcher;ngControl;_parentFormGroup;_parentForm;_stateChanges;errorState=!1;matcher;constructor(t,e,i,r,o){this._defaultMatcher=t,this.ngControl=e,this._parentFormGroup=i,this._parentForm=r,this._stateChanges=o}updateErrorState(){let t=this.errorState,e=this._parentFormGroup||this._parentForm,i=this.matcher||this._defaultMatcher,r=this.ngControl?this.ngControl.control:null,o=i?.isErrorState(r,e)??!1;o!==t&&(this.errorState=o,this._stateChanges.next())}};var GN=["button","checkbox","file","hidden","image","radio","range","reset","submit"],WN=new v("MAT_INPUT_CONFIG"),us=(()=>{class n{_elementRef=u(P);_platform=u(_e);ngControl=u(En,{optional:!0,self:!0});_autofillMonitor=u(aE);_ngZone=u(F);_formField=u(ol,{optional:!0});_renderer=u(Ke);_uid=u(Ie).getId("mat-input-");_previousNativeValue;_inputValueAccessor;_signalBasedValueAccessor;_previousPlaceholder=null;_errorStateTracker;_config=u(WN,{optional:!0});_cleanupIosKeyup;_cleanupWebkitWheel;_isServer=!1;_isNativeSelect=!1;_isTextarea=!1;_isInFormField=!1;focused=!1;stateChanges=new k;controlType="mat-input";autofilled=!1;get disabled(){return this._disabled}set disabled(e){this._disabled=fi(e),this.focused&&(this.focused=!1,this.stateChanges.next())}_disabled=!1;get id(){return this._id}set id(e){this._id=e||this._uid}_id;placeholder;name;get required(){return this._required??this.ngControl?.control?.hasValidator(Zo.required)??!1}set required(e){this._required=fi(e)}_required;get type(){return this._type}set type(e){this._type=e||"text",this._validateType(),!this._isTextarea&&p_().has(this._type)&&(this._elementRef.nativeElement.type=this._type)}_type="text";get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}userAriaDescribedBy;get value(){return this._signalBasedValueAccessor?this._signalBasedValueAccessor.value():this._inputValueAccessor.value}set value(e){e!==this.value&&(this._signalBasedValueAccessor?this._signalBasedValueAccessor.value.set(e):this._inputValueAccessor.value=e,this.stateChanges.next())}get readonly(){return this._readonly}set readonly(e){this._readonly=fi(e)}_readonly=!1;disabledInteractive;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}_neverEmptyInputTypes=["date","datetime","datetime-local","month","time","week"].filter(e=>p_().has(e));constructor(){let e=u(Ga,{optional:!0}),i=u($r,{optional:!0}),r=u(cs),o=u(cE,{optional:!0,self:!0}),s=this._elementRef.nativeElement,a=s.nodeName.toLowerCase();o?Hn(o.value)?this._signalBasedValueAccessor=o:this._inputValueAccessor=o:this._inputValueAccessor=s,this._previousNativeValue=this.value,this.id=this.id,this._platform.IOS&&this._ngZone.runOutsideAngular(()=>{this._cleanupIosKeyup=this._renderer.listen(s,"keyup",this._iOSKeyupListener)}),this._errorStateTracker=new ds(r,this.ngControl,i,e,this.stateChanges),this._isServer=!this._platform.isBrowser,this._isNativeSelect=a==="select",this._isTextarea=a==="textarea",this._isInFormField=!!this._formField,this.disabledInteractive=this._config?.disabledInteractive||!1,this._isNativeSelect&&(this.controlType=s.multiple?"mat-native-select-multiple":"mat-native-select"),this._signalBasedValueAccessor&&Fi(()=>{this._signalBasedValueAccessor.value(),this.stateChanges.next()})}ngAfterViewInit(){this._platform.isBrowser&&this._autofillMonitor.monitor(this._elementRef.nativeElement).subscribe(e=>{this.autofilled=e.isAutofilled,this.stateChanges.next()})}ngOnChanges(){this.stateChanges.next()}ngOnDestroy(){this.stateChanges.complete(),this._platform.isBrowser&&this._autofillMonitor.stopMonitoring(this._elementRef.nativeElement),this._cleanupIosKeyup?.(),this._cleanupWebkitWheel?.()}ngDoCheck(){this.ngControl&&(this.updateErrorState(),this.ngControl.disabled!==null&&this.ngControl.disabled!==this.disabled&&(this.disabled=this.ngControl.disabled,this.stateChanges.next())),this._dirtyCheckNativeValue(),this._dirtyCheckPlaceholder()}focus(e){this._elementRef.nativeElement.focus(e)}updateErrorState(){this._errorStateTracker.updateErrorState()}_focusChanged(e){if(e!==this.focused){if(!this._isNativeSelect&&e&&this.disabled&&this.disabledInteractive){let i=this._elementRef.nativeElement;i.type==="number"?(i.type="text",i.setSelectionRange(0,0),i.type="number"):i.setSelectionRange(0,0)}this.focused=e,this.stateChanges.next()}}_onInput(){}_dirtyCheckNativeValue(){let e=this._elementRef.nativeElement.value;this._previousNativeValue!==e&&(this._previousNativeValue=e,this.stateChanges.next())}_dirtyCheckPlaceholder(){let e=this._getPlaceholder();if(e!==this._previousPlaceholder){let i=this._elementRef.nativeElement;this._previousPlaceholder=e,e?i.setAttribute("placeholder",e):i.removeAttribute("placeholder")}}_getPlaceholder(){return this.placeholder||null}_validateType(){GN.indexOf(this._type)>-1}_isNeverEmpty(){return this._neverEmptyInputTypes.indexOf(this._type)>-1}_isBadInput(){let e=this._elementRef.nativeElement.validity;return e&&e.badInput}get empty(){return!this._isNeverEmpty()&&!this._elementRef.nativeElement.value&&!this._isBadInput()&&!this.autofilled}get shouldLabelFloat(){if(this._isNativeSelect){let e=this._elementRef.nativeElement,i=e.options[0];return this.focused||e.multiple||!this.empty||!!(e.selectedIndex>-1&&i&&i.label)}else return this.focused&&!this.disabled||!this.empty}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(){this.focused||this.focus()}_isInlineSelect(){let e=this._elementRef.nativeElement;return this._isNativeSelect&&(e.multiple||e.size>1)}_iOSKeyupListener=e=>{let i=e.target;!i.value&&i.selectionStart===0&&i.selectionEnd===0&&(i.setSelectionRange(1,1),i.setSelectionRange(0,0))};_getReadonlyAttribute(){return this._isNativeSelect?null:this.readonly||this.disabled&&this.disabledInteractive?"true":null}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["input","matInput",""],["textarea","matInput",""],["select","matNativeControl",""],["input","matNativeControl",""],["textarea","matNativeControl",""]],hostAttrs:[1,"mat-mdc-input-element"],hostVars:21,hostBindings:function(i,r){i&1&&V("focus",function(){return r._focusChanged(!0)})("blur",function(){return r._focusChanged(!1)})("input",function(){return r._onInput()}),i&2&&(Nt("id",r.id)("disabled",r.disabled&&!r.disabledInteractive)("required",r.required),J("name",r.name||null)("readonly",r._getReadonlyAttribute())("aria-disabled",r.disabled&&r.disabledInteractive?"true":null)("aria-invalid",r.empty&&r.required?null:r.errorState)("aria-required",r.required)("id",r.id),j("mat-input-server",r._isServer)("mat-mdc-form-field-textarea-control",r._isInFormField&&r._isTextarea)("mat-mdc-form-field-input-control",r._isInFormField)("mat-mdc-input-disabled-interactive",r.disabledInteractive)("mdc-text-field__input",r._isInFormField)("mat-mdc-native-select-inline",r._isInlineSelect()))},inputs:{disabled:"disabled",id:"id",placeholder:"placeholder",name:"name",required:"required",type:"type",errorStateMatcher:"errorStateMatcher",userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],value:"value",readonly:"readonly",disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},exportAs:["matInput"],features:[ve([{provide:rl,useExisting:n}]),Le]})}return n})(),sl=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[Yi,Yi,lE,oe]})}return n})();var dE=(()=>{class n{constructor(e,i){this.questionsService=e,this.httpClient=i,this.pageToShowEvent=new z}ngOnInit(){this.questionsService.questionsReplaySubject.subscribe(e=>this.questions=e),this.httpClient.post("https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net","\u042F \u0437\u0430\u043F\u043E\u043B\u043D\u044F\u044E (\u041A\u0441\u0435\u043D\u0438\u044F)").subscribe(e=>{console.log("")})}goToPage2(){this.questionsService.questionsReplaySubject.next(this.questions),this.pageToShowEvent.emit(2)}static{this.\u0275fac=function(i){return new(i||n)(q(Gi),q(ln))}}static{this.\u0275cmp=A({type:n,selectors:[["app-page1"]],inputs:{cameFromStart:"cameFromStart"},outputs:{pageToShowEvent:"pageToShowEvent"},standalone:!1,decls:41,vars:0,consts:[[1,"mainPage1"],[1,"leftPage1"],["src","assets/first.jpg"],[1,"rightPage1"],["mat-raised-button","","color","accent","id","continue-button",3,"click"]],template:function(i,r){i&1&&(_(0,"div",0)(1,"div",1),Z(2,"img",2),g(),_(3,"div",3)(4,"p"),E(5,"\u0418\u0442\u0430\u043A, \u043D\u0430\u0447\u043D\u0435\u043C. \u0422.\u043A. \u0432 \u0440\u0435\u0430\u043B\u044C\u043D\u043E\u0441\u0442\u0438 \u043E\u0431\u044B\u0447\u043D\u043E \u0447\u0435\u043B\u043E\u0432\u0435\u043A \u0441\u0442\u0435\u0441\u043D\u044F\u0435\u0442\u0441\u044F \u0440\u0430\u0441\u0441\u043A\u0430\u0437\u0430\u0442\u044C \u043E \u0441\u0432\u043E\u0438\u0445 \u0444\u0430\u043D\u0442\u0430\u0437\u0438\u044F\u0445 \u0438/\u0438\u043B\u0438 \u043E\u043F\u0430\u0441\u0435\u043D\u0438\u044F\u0445, \u0442\u043E \u043B\u0443\u0447\u0448\u0435 \u043E\u0442\u0432\u0435\u0442\u0438\u0442\u044C \u043D\u0430 \u0432\u043E\u043F\u0440\u043E\u0441\u044B \u0437\u0434\u0435\u0441\u044C. \u0414\u0430\u0436\u0435 \u043C\u043E\u0436\u0435\u0442 \u0432 \u0447\u0435\u043C-\u0442\u043E \u0441\u0435\u0431\u0435 \u043F\u0440\u0438\u0437\u043D\u0430\u0442\u044C\u0441\u044F, \u043F\u043E\u044D\u0442\u043E\u043C\u0443 \u043E\u0442\u0432\u0435\u0447\u0430\u0442\u044C \u043D\u0443\u0436\u043D\u043E \u043C\u0430\u043A\u0441\u0438\u043C\u0430\u043B\u044C\u043D\u043E \u0447\u0435\u0441\u0442\u043D\u043E. \u041D\u0430 \u0441\u043B\u0435\u0434\u0443\u044E\u0449\u0435\u0439 \u0441\u0442\u0440\u0430\u043D\u0438\u0446\u0435 \u044F \u0441\u043E\u0431\u0440\u0430\u043B \u0440\u0430\u0437\u043D\u044B\u0435 \u0434\u0435\u0439\u0441\u0442\u0432\u0438\u044F \u0432 \u0411\u0414\u0421\u041C (\u0438 \u043C\u0435\u0436\u0434\u0443 \u043D\u0438\u043C\u0438 \u043D\u0435\u0441\u043A\u043E\u043B\u044C\u043A\u043E \u043C\u043E\u0438\u0445 \u0432\u043E\u043F\u0440\u043E\u0441\u043E\u0432). \u0423\u043A\u0430\u0436\u0438 \u0441\u0432\u043E\u0435 \u043E\u0442\u043D\u043E\u0448\u0435\u043D\u0438\u0435 \u043A \u043A\u0430\u0436\u0434\u043E\u043C\u0443 \u043F\u0443\u043D\u043A\u0442\u0443 \u043E\u043F\u0440\u043E\u0441\u043D\u0438\u043A\u0430 \u0431\u0430\u043B\u0430\u043C\u0438 \u043E\u0442 0 \u0434\u043E 5. \u0414\u043B\u044F \u0447\u0438\u0441\u0442\u043E\u0442\u044B \u044D\u043A\u0441\u043F\u0435\u0440\u0438\u043C\u0435\u043D\u0442\u0430 \u0431\u0443\u0434\u0435\u043C \u0441\u0447\u0438\u0442\u0430\u0442\u044C, \u0447\u0442\u043E \u0442\u044B \u0441\u043E\u0431\u0435\u0441\u0435\u0434\u0443\u0435\u0448\u044C\u0441\u044F \u043D\u0430 \u0434\u043E\u043B\u0436\u043D\u043E\u0441\u0442\u044C \u043C\u043E\u0435\u0439 \u0440\u0430\u0431\u044B\u043D\u0438. \u041E\u0442\u043D\u0435\u0441\u0438\u0441\u044C \u0441\u0435\u0440\u044C\u0435\u0437\u043D\u043E, \u0442.\u043A. \u044F \u0431\u0443\u0434\u0443 \u0440\u0443\u043A\u043E\u0432\u043E\u0434\u0441\u0442\u0432\u043E\u0432\u0430\u0442\u044C\u0441\u044F \u0442\u0432\u043E\u0438\u043C\u0438 \u043E\u0442\u0432\u0435\u0442\u0430\u043C\u0438, \u043F\u043E\u0442\u043E\u043C \u0447\u0442\u043E\u0431\u044B \u043D\u0435 \u0436\u0430\u043B\u043E\u0432\u0430\u043B\u0430\u0441\u044C)) "),g(),_(6,"p"),E(7,"\u0414\u043B\u044F \u043D\u0430\u0447\u0430\u043B\u0430 \u0440\u0430\u0437\u044A\u044F\u0441\u043D\u0435\u043D\u0438\u044F \u0431\u0430\u043B\u043B\u043E\u0432:"),g(),_(8,"ul")(9,"li")(10,"strong"),E(11,"0"),g(),E(12," - \u044D\u0442\u043E \u0442\u0430\u0431\u0443"),g(),_(13,"li")(14,"strong"),E(15,"1"),g(),E(16," - \u0441\u043E\u0432\u0441\u0435\u043C \u043D\u0435 \u043D\u0440\u0430\u0432\u0438\u0442\u0441\u044F \u044D\u0442\u043E\u0442 \u043F\u0443\u043D\u043A\u0442. \u041D\u043E \u0412\u0435\u0440\u0445\u043D\u044F\u044F \u043C\u043E\u0436\u0435\u0442 \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u043E\u0432\u0430\u0442\u044C, \u0435\u0441\u043B\u0438 \u0440\u0430\u0431 \u043E\u0447\u0435\u043D\u044C \u0441\u0438\u043B\u044C\u043D\u043E \u043F\u0440\u043E\u0432\u0438\u043D\u0438\u043B\u0430\u0441\u044C"),g(),_(17,"li")(18,"strong"),E(19,"2"),g(),E(20," - \u0442\u044B \u043D\u0435 \u043E\u0447\u0435\u043D\u044C \u0445\u043E\u0447\u0435\u0448\u044C/\u043B\u044E\u0431\u0438\u0448\u044C \u044D\u0442\u043E\u0442 \u043F\u0443\u043D\u043A\u0442, \u043D\u043E \u044F \u043C\u043E\u0433\u0443 \u0435\u0433\u043E \u043D\u0435\u0447\u0430\u0441\u0442\u043E/\u043D\u0435\u0434\u043E\u043B\u0433\u043E \u0438\u0441\u043F\u043E\u043B\u044C\u0437\u043E\u0432\u0430\u0442\u044C \u043D\u0430 \u0441\u0432\u043E\u0435 \u0443\u0441\u043C\u043E\u0442\u0440\u0435\u043D\u0438\u0435"),g(),_(21,"li")(22,"strong"),E(23,"3"),g(),E(24," - \u0442\u044B \u043E\u0442\u043D\u043E\u0441\u0438\u0448\u044C\u0441\u044F \u043A \u044D\u0442\u043E\u043C\u0443 \u043F\u0443\u043D\u043A\u0442\u0443 \u043D\u0435\u0439\u0442\u0440\u0430\u043B\u044C\u043D\u043E"),g(),_(25,"li")(26,"strong"),E(27,"4"),g(),E(28," - \u0442\u0435\u0431\u0435 \u043D\u0440\u0430\u0432\u0438\u0442\u0441\u044F \u044D\u0442\u043E\u0442 \u043F\u0443\u043D\u043A\u0442/\u0445\u043E\u0447\u0435\u0448\u044C \u0435\u0433\u043E \u043F\u043E\u043F\u0440\u043E\u0431\u043E\u0432\u0430\u0442\u044C"),g(),_(29,"li")(30,"strong"),E(31,"5"),g(),E(32," - \u044D\u0442\u043E \u043E\u0434\u0438\u043D \u0438\u0437 \u0442\u0432\u043E\u0438\u0445 \u043B\u044E\u0431\u0438\u043C\u044B\u0445/\u0436\u0435\u043B\u0430\u043D\u043D\u044B\u0445 \u043F\u043E\u043F\u0440\u043E\u0431\u043E\u0432\u0430\u0442\u044C \u043F\u0443\u043D\u043A\u0442\u043E\u0432. \u0415\u0441\u043B\u0438 \u0442\u044B \u0432\u0435\u043B\u0430 \u0441\u0435\u0431\u044F \u0445\u043E\u0440\u043E\u0448\u043E, \u0442\u043E \u044F \u043F\u043E\u0434\u0443\u043C\u0430\u044E, \u0447\u0442\u043E\u0431\u044B \u0445\u043E\u0442\u044F \u0431\u044B \u0440\u0430\u0437 \u0437\u0430 \u0432\u0441\u0442\u0440\u0435\u0447\u0443 \u043F\u0440\u0438\u043C\u0435\u043D\u0438\u0442\u044C \u043F\u0443\u043D\u043A\u0442 \u0441 \u0442\u0430\u043A\u043E\u0439 \u043E\u0446\u0435\u043D\u043A\u043E\u0439"),g(),_(33,"li")(34,"strong"),E(35,"\u0422\u0432\u043E\u0439 \u0432\u0430\u0440\u0438\u0430\u043D\u0442"),g(),E(36," - \u0432 \u043A\u043E\u043D\u0446\u0435 \u0430\u043D\u043A\u0435\u0442\u044B \u0442\u044B \u0434\u043E\u043B\u0436\u043D\u0430 \u0437\u0430\u043F\u043E\u043B\u043D\u0438\u0442\u044C 3 \u0441\u0432\u043E\u0438 \u0444\u0430\u043D\u0442\u0430\u0437\u0438\u0438, \u043A\u043E\u0442\u043E\u0440\u044B\u0435 \u0445\u043E\u0442\u0435\u043B\u0430 \u0431\u044B \u0440\u0435\u0430\u043B\u0438\u0437\u043E\u0432\u0430\u0442\u044C (\u043C\u043E\u0436\u043D\u043E \u043D\u0435 \u0442\u043E\u043B\u044C\u043A\u043E \u043A\u043E\u0433\u0434\u0430 \u0442\u044B \u0441\u043D\u0438\u0437\u0443/\u043F\u043E \u0422\u0435\u043C\u0435)"),g()(),_(37,"p"),E(38,"P.S. \u041A \u043A\u0430\u0436\u0434\u043E\u043C\u0443 \u0432\u043E\u043F\u0440\u043E\u0441\u0443 \u043C\u043E\u0436\u043D\u043E \u0434\u043E\u0431\u0430\u0432\u0438\u0442\u044C \u043A\u043E\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439, \u0435\u0441\u043B\u0438 \u0437\u0430\u0445\u043E\u0447\u0435\u0448\u044C \u0447\u0442\u043E-\u0442\u043E \u0443\u0442\u043E\u0447\u043D\u0438\u0442\u044C"),g(),_(39,"button",4),V("click",function(){return r.goToPage2()}),E(40," \u041F\u043E\u043D\u044F\u043B\u0430 "),g()()())},dependencies:[Gn],styles:["@media(max-width:600px){.mainPage1[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;justify-content:center}.leftPage1[_ngcontent-%COMP%]{width:100%}.rightPage1[_ngcontent-%COMP%]{width:100%;display:flex;flex-direction:column;align-items:center;justify-content:center}}@media screen and (min-width:600px){.mainPage1[_ngcontent-%COMP%]{display:flex;flex-direction:row;align-items:center;justify-content:center}.leftPage1[_ngcontent-%COMP%]{width:30%}.rightPage1[_ngcontent-%COMP%]{width:68%;display:flex;flex-direction:column;align-items:center;justify-content:center}}"]})}}return n})();var al=class{};function ll(n){return n&&typeof n.connect=="function"&&!(n instanceof Ds)}var kn=(function(n){return n[n.REPLACED=0]="REPLACED",n[n.INSERTED=1]="INSERTED",n[n.MOVED=2]="MOVED",n[n.REMOVED=3]="REMOVED",n})(kn||{}),Ku=class{viewCacheSize=20;_viewCache=[];applyChanges(t,e,i,r,o){t.forEachOperation((s,a,l)=>{let c,d;if(s.previousIndex==null){let m=()=>i(s,a,l);c=this._insertView(m,l,e,r(s)),d=c?kn.INSERTED:kn.REPLACED}else l==null?(this._detachAndCacheView(a,e),d=kn.REMOVED):(c=this._moveView(a,l,e,r(s)),d=kn.MOVED);o&&o({context:c?.context,operation:d,record:s})})}detach(){for(let t of this._viewCache)t.destroy();this._viewCache=[]}_insertView(t,e,i,r){let o=this._insertViewFromCache(e,i);if(o){o.context.$implicit=r;return}let s=t();return i.createEmbeddedView(s.templateRef,s.context,s.index)}_detachAndCacheView(t,e){let i=e.detach(t);this._maybeCacheView(i,e)}_moveView(t,e,i,r){let o=i.get(t);return i.move(o,e),o.context.$implicit=r,o}_maybeCacheView(t,e){if(this._viewCache.length<this.viewCacheSize)this._viewCache.push(t);else{let i=e.indexOf(t);i===-1?t.destroy():e.remove(i)}}_insertViewFromCache(t,e){let i=this._viewCache.pop();return i&&e.insert(i,t),i||null}};var Kr=class{_multiple;_emitChanges;compareWith;_selection=new Set;_deselectedToEmit=[];_selectedToEmit=[];_selected=null;get selected(){return this._selected||(this._selected=Array.from(this._selection.values())),this._selected}changed=new k;constructor(t=!1,e,i=!0,r){this._multiple=t,this._emitChanges=i,this.compareWith=r,e&&e.length&&(t?e.forEach(o=>this._markSelected(o)):this._markSelected(e[0]),this._selectedToEmit.length=0)}select(...t){this._verifyValueAssignment(t),t.forEach(i=>this._markSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}deselect(...t){this._verifyValueAssignment(t),t.forEach(i=>this._unmarkSelected(i));let e=this._hasQueuedChanges();return this._emitChangeEvent(),e}setSelection(...t){this._verifyValueAssignment(t);let e=this.selected,i=new Set(t.map(o=>this._getConcreteValue(o)));t.forEach(o=>this._markSelected(o)),e.filter(o=>!i.has(this._getConcreteValue(o,i))).forEach(o=>this._unmarkSelected(o));let r=this._hasQueuedChanges();return this._emitChangeEvent(),r}toggle(t){return this.isSelected(t)?this.deselect(t):this.select(t)}clear(t=!0){this._unmarkAll();let e=this._hasQueuedChanges();return t&&this._emitChangeEvent(),e}isSelected(t){return this._selection.has(this._getConcreteValue(t))}isEmpty(){return this._selection.size===0}hasValue(){return!this.isEmpty()}sort(t){this._multiple&&this.selected&&this._selected.sort(t)}isMultipleSelection(){return this._multiple}_emitChangeEvent(){this._selected=null,(this._selectedToEmit.length||this._deselectedToEmit.length)&&(this.changed.next({source:this,added:this._selectedToEmit,removed:this._deselectedToEmit}),this._deselectedToEmit=[],this._selectedToEmit=[])}_markSelected(t){t=this._getConcreteValue(t),this.isSelected(t)||(this._multiple||this._unmarkAll(),this.isSelected(t)||this._selection.add(t),this._emitChanges&&this._selectedToEmit.push(t))}_unmarkSelected(t){t=this._getConcreteValue(t),this.isSelected(t)&&(this._selection.delete(t),this._emitChanges&&this._deselectedToEmit.push(t))}_unmarkAll(){this.isEmpty()||this._selection.forEach(t=>this._unmarkSelected(t))}_verifyValueAssignment(t){t.length>1&&this._multiple}_hasQueuedChanges(){return!!(this._deselectedToEmit.length||this._selectedToEmit.length)}_getConcreteValue(t,e){if(this.compareWith){e=e??this._selection;for(let i of e)if(this.compareWith(t,i))return i;return t}else return t}};var Yu=class{applyChanges(t,e,i,r,o){t.forEachOperation((s,a,l)=>{let c,d;if(s.previousIndex==null){let m=i(s,a,l);c=e.createEmbeddedView(m.templateRef,m.context,m.index),d=kn.INSERTED}else l==null?(e.remove(a),d=kn.REMOVED):(c=e.get(a),e.move(c,l),d=kn.MOVED);o&&o({context:c?.context,operation:d,record:s})})}detach(){}};var QN=20,Yr=(()=>{class n{_ngZone=u(F);_platform=u(_e);_renderer=u(qe).createRenderer(null,null);_cleanupGlobalListener;constructor(){}_scrolled=new k;_scrolledCount=0;scrollContainers=new Map;register(e){this.scrollContainers.has(e)||this.scrollContainers.set(e,e.elementScrolled().subscribe(()=>this._scrolled.next(e)))}deregister(e){let i=this.scrollContainers.get(e);i&&(i.unsubscribe(),this.scrollContainers.delete(e))}scrolled(e=QN){return this._platform.isBrowser?new ae(i=>{this._cleanupGlobalListener||(this._cleanupGlobalListener=this._ngZone.runOutsideAngular(()=>this._renderer.listen("document","scroll",()=>this._scrolled.next())));let r=e>0?this._scrolled.pipe(ho(e)).subscribe(i):this._scrolled.subscribe(i);return this._scrolledCount++,()=>{r.unsubscribe(),this._scrolledCount--,this._scrolledCount||(this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0)}}):$e()}ngOnDestroy(){this._cleanupGlobalListener?.(),this._cleanupGlobalListener=void 0,this.scrollContainers.forEach((e,i)=>this.deregister(i)),this._scrolled.complete()}ancestorScrolled(e,i){let r=this.getAncestorScrollContainers(e);return this.scrolled(i).pipe(ze(o=>!o||r.indexOf(o)>-1))}getAncestorScrollContainers(e){let i=[];return this.scrollContainers.forEach((r,o)=>{this._scrollableContainsElement(o,e)&&i.push(o)}),i}_scrollableContainsElement(e,i){let r=Wt(i),o=e.getElementRef().nativeElement;do if(r==o)return!0;while(r=r.parentElement);return!1}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var KN=20,Sn=(()=>{class n{_platform=u(_e);_listeners;_viewportSize=null;_change=new k;_document=u(U);constructor(){let e=u(F),i=u(qe).createRenderer(null,null);e.runOutsideAngular(()=>{if(this._platform.isBrowser){let r=o=>this._change.next(o);this._listeners=[i.listen("window","resize",r),i.listen("window","orientationchange",r)]}this.change().subscribe(()=>this._viewportSize=null)})}ngOnDestroy(){this._listeners?.forEach(e=>e()),this._change.complete()}getViewportSize(){this._viewportSize||this._updateViewportSize();let e={width:this._viewportSize.width,height:this._viewportSize.height};return this._platform.isBrowser||(this._viewportSize=null),e}getViewportRect(){let e=this.getViewportScrollPosition(),{width:i,height:r}=this.getViewportSize();return{top:e.top,left:e.left,bottom:e.top+r,right:e.left+i,height:r,width:i}}getViewportScrollPosition(){if(!this._platform.isBrowser)return{top:0,left:0};let e=this._document,i=this._getWindow(),r=e.documentElement,o=r.getBoundingClientRect(),s=-o.top||e.body?.scrollTop||i.scrollY||r.scrollTop||0,a=-o.left||e.body?.scrollLeft||i.scrollX||r.scrollLeft||0;return{top:s,left:a}}change(e=KN){return e>0?this._change.pipe(ho(e)):this._change}_getWindow(){return this._document.defaultView||window}_updateViewportSize(){let e=this._getWindow();this._viewportSize=this._platform.isBrowser?{width:e.innerWidth,height:e.innerHeight}:{width:0,height:0}}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var uE=new v("CDK_VIRTUAL_SCROLL_VIEWPORT");var qn=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})(),cl=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe,qn,oe,qn]})}return n})();var dl=class{_attachedHost=null;attach(t){return this._attachedHost=t,t.attach(this)}detach(){let t=this._attachedHost;t!=null&&(this._attachedHost=null,t.detach())}get isAttached(){return this._attachedHost!=null}setAttachedHost(t){this._attachedHost=t}},In=class extends dl{component;viewContainerRef;injector;projectableNodes;bindings;constructor(t,e,i,r,o){super(),this.component=t,this.viewContainerRef=e,this.injector=i,this.projectableNodes=r,this.bindings=o||null}},gi=class extends dl{templateRef;viewContainerRef;context;injector;constructor(t,e,i,r){super(),this.templateRef=t,this.viewContainerRef=e,this.context=i,this.injector=r}get origin(){return this.templateRef.elementRef}attach(t,e=this.context){return this.context=e,super.attach(t)}detach(){return this.context=void 0,super.detach()}},M_=class extends dl{element;constructor(t){super(),this.element=t instanceof P?t.nativeElement:t}},Zi=class{_attachedPortal=null;_disposeFn=null;_isDisposed=!1;hasAttached(){return!!this._attachedPortal}attach(t){if(t instanceof In)return this._attachedPortal=t,this.attachComponentPortal(t);if(t instanceof gi)return this._attachedPortal=t,this.attachTemplatePortal(t);if(this.attachDomPortal&&t instanceof M_)return this._attachedPortal=t,this.attachDomPortal(t)}attachDomPortal=null;detach(){this._attachedPortal&&(this._attachedPortal.setAttachedHost(null),this._attachedPortal=null),this._invokeDisposeFn()}dispose(){this.hasAttached()&&this.detach(),this._invokeDisposeFn(),this._isDisposed=!0}setDisposeFn(t){this._disposeFn=t}_invokeDisposeFn(){this._disposeFn&&(this._disposeFn(),this._disposeFn=null)}},Zu=class extends Zi{outletElement;_appRef;_defaultInjector;constructor(t,e,i){super(),this.outletElement=t,this._appRef=e,this._defaultInjector=i}attachComponentPortal(t){let e;if(t.viewContainerRef){let i=t.injector||t.viewContainerRef.injector,r=i.get(jn,null,{optional:!0})||void 0;e=t.viewContainerRef.createComponent(t.component,{index:t.viewContainerRef.length,injector:i,ngModuleRef:r,projectableNodes:t.projectableNodes||void 0,bindings:t.bindings||void 0}),this.setDisposeFn(()=>e.destroy())}else{let i=this._appRef,r=t.injector||this._defaultInjector||$.NULL,o=r.get(Ze,i.injector);e=Bd(t.component,{elementInjector:r,environmentInjector:o,projectableNodes:t.projectableNodes||void 0,bindings:t.bindings||void 0}),i.attachView(e.hostView),this.setDisposeFn(()=>{i.viewCount>0&&i.detachView(e.hostView),e.destroy()})}return this.outletElement.appendChild(this._getComponentRootNode(e)),this._attachedPortal=t,e}attachTemplatePortal(t){let e=t.viewContainerRef,i=e.createEmbeddedView(t.templateRef,t.context,{injector:t.injector});return i.rootNodes.forEach(r=>this.outletElement.appendChild(r)),i.detectChanges(),this.setDisposeFn(()=>{let r=e.indexOf(i);r!==-1&&e.remove(r)}),this._attachedPortal=t,i}attachDomPortal=t=>{let e=t.element;e.parentNode;let i=this.outletElement.ownerDocument.createComment("dom-portal");e.parentNode.insertBefore(i,e),this.outletElement.appendChild(e),this._attachedPortal=t,super.setDisposeFn(()=>{i.parentNode&&i.parentNode.replaceChild(e,i)})};dispose(){super.dispose(),this.outletElement.remove()}_getComponentRootNode(t){return t.hostView.rootNodes[0]}};var Xi=(()=>{class n extends Zi{_moduleRef=u(jn,{optional:!0});_document=u(U);_viewContainerRef=u(tt);_isInitialized=!1;_attachedRef=null;constructor(){super()}get portal(){return this._attachedPortal}set portal(e){this.hasAttached()&&!e&&!this._isInitialized||(this.hasAttached()&&super.detach(),e&&super.attach(e),this._attachedPortal=e||null)}attached=new z;get attachedRef(){return this._attachedRef}ngOnInit(){this._isInitialized=!0}ngOnDestroy(){super.dispose(),this._attachedRef=this._attachedPortal=null}attachComponentPortal(e){e.setAttachedHost(this);let i=e.viewContainerRef!=null?e.viewContainerRef:this._viewContainerRef,r=i.createComponent(e.component,{index:i.length,injector:e.injector||i.injector,projectableNodes:e.projectableNodes||void 0,ngModuleRef:this._moduleRef||void 0,bindings:e.bindings||void 0});return i!==this._viewContainerRef&&this._getRootNode().appendChild(r.hostView.rootNodes[0]),super.setDisposeFn(()=>r.destroy()),this._attachedPortal=e,this._attachedRef=r,this.attached.emit(r),r}attachTemplatePortal(e){e.setAttachedHost(this);let i=this._viewContainerRef.createEmbeddedView(e.templateRef,e.context,{injector:e.injector});return super.setDisposeFn(()=>this._viewContainerRef.clear()),this._attachedPortal=e,this._attachedRef=i,this.attached.emit(i),i}attachDomPortal=e=>{let i=e.element;i.parentNode;let r=this._document.createComment("dom-portal");e.setAttachedHost(this),i.parentNode.insertBefore(r,i),this._getRootNode().appendChild(i),this._attachedPortal=e,super.setDisposeFn(()=>{r.parentNode&&r.parentNode.replaceChild(i,r)})};_getRootNode(){let e=this._viewContainerRef.element.nativeElement;return e.nodeType===e.ELEMENT_NODE?e:e.parentNode}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkPortalOutlet",""]],inputs:{portal:[0,"cdkPortalOutlet","portal"]},outputs:{attached:"attached"},exportAs:["cdkPortalOutlet"],features:[re]})}return n})(),Ji=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({})}return n})();var mE=Nx();function fs(n){return new Xu(n.get(Sn),n.get(U))}var Xu=class{_viewportRuler;_previousHTMLStyles={top:"",left:""};_previousScrollPosition;_isEnabled=!1;_document;constructor(t,e){this._viewportRuler=t,this._document=e}attach(){}enable(){if(this._canBeEnabled()){let t=this._document.documentElement;this._previousScrollPosition=this._viewportRuler.getViewportScrollPosition(),this._previousHTMLStyles.left=t.style.left||"",this._previousHTMLStyles.top=t.style.top||"",t.style.left=et(-this._previousScrollPosition.left),t.style.top=et(-this._previousScrollPosition.top),t.classList.add("cdk-global-scrollblock"),this._isEnabled=!0}}disable(){if(this._isEnabled){let t=this._document.documentElement,e=this._document.body,i=t.style,r=e.style,o=i.scrollBehavior||"",s=r.scrollBehavior||"";this._isEnabled=!1,i.left=this._previousHTMLStyles.left,i.top=this._previousHTMLStyles.top,t.classList.remove("cdk-global-scrollblock"),mE&&(i.scrollBehavior=r.scrollBehavior="auto"),window.scroll(this._previousScrollPosition.left,this._previousScrollPosition.top),mE&&(i.scrollBehavior=o,r.scrollBehavior=s)}}_canBeEnabled(){if(this._document.documentElement.classList.contains("cdk-global-scrollblock")||this._isEnabled)return!1;let e=this._document.documentElement,i=this._viewportRuler.getViewportSize();return e.scrollHeight>i.height||e.scrollWidth>i.width}};function bE(n,t){return new Ju(n.get(Yr),n.get(F),n.get(Sn),t)}var Ju=class{_scrollDispatcher;_ngZone;_viewportRuler;_config;_scrollSubscription=null;_overlayRef;_initialScrollPosition;constructor(t,e,i,r){this._scrollDispatcher=t,this._ngZone=e,this._viewportRuler=i,this._config=r}attach(t){this._overlayRef,this._overlayRef=t}enable(){if(this._scrollSubscription)return;let t=this._scrollDispatcher.scrolled(0).pipe(ze(e=>!e||!this._overlayRef.overlayElement.contains(e.getElementRef().nativeElement)));this._config&&this._config.threshold&&this._config.threshold>1?(this._initialScrollPosition=this._viewportRuler.getViewportScrollPosition().top,this._scrollSubscription=t.subscribe(()=>{let e=this._viewportRuler.getViewportScrollPosition().top;Math.abs(e-this._initialScrollPosition)>this._config.threshold?this._detach():this._overlayRef.updatePosition()})):this._scrollSubscription=t.subscribe(this._detach)}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}_detach=()=>{this.disable(),this._overlayRef.hasAttached()&&this._ngZone.run(()=>this._overlayRef.detach())}};var ul=class{enable(){}disable(){}attach(){}};function T_(n,t){return t.some(e=>{let i=n.bottom<e.top,r=n.top>e.bottom,o=n.right<e.left,s=n.left>e.right;return i||r||o||s})}function hE(n,t){return t.some(e=>{let i=n.top<e.top,r=n.bottom>e.bottom,o=n.left<e.left,s=n.right>e.right;return i||r||o||s})}function Jr(n,t){return new em(n.get(Yr),n.get(Sn),n.get(F),t)}var em=class{_scrollDispatcher;_viewportRuler;_ngZone;_config;_scrollSubscription=null;_overlayRef;constructor(t,e,i,r){this._scrollDispatcher=t,this._viewportRuler=e,this._ngZone=i,this._config=r}attach(t){this._overlayRef,this._overlayRef=t}enable(){if(!this._scrollSubscription){let t=this._config?this._config.scrollThrottle:0;this._scrollSubscription=this._scrollDispatcher.scrolled(t).subscribe(()=>{if(this._overlayRef.updatePosition(),this._config&&this._config.autoClose){let e=this._overlayRef.overlayElement.getBoundingClientRect(),{width:i,height:r}=this._viewportRuler.getViewportSize();T_(e,[{width:i,height:r,bottom:r,right:i,top:0,left:0}])&&(this.disable(),this._ngZone.run(()=>this._overlayRef.detach()))}})}}disable(){this._scrollSubscription&&(this._scrollSubscription.unsubscribe(),this._scrollSubscription=null)}detach(){this.disable(),this._overlayRef=null}},yE=(()=>{class n{_injector=u($);constructor(){}noop=()=>new ul;close=e=>bE(this._injector,e);block=()=>fs(this._injector);reposition=e=>Jr(this._injector,e);static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),_i=class{positionStrategy;scrollStrategy=new ul;panelClass="";hasBackdrop=!1;backdropClass="cdk-overlay-dark-backdrop";disableAnimations;width;height;minWidth;minHeight;maxWidth;maxHeight;direction;disposeOnNavigation=!1;usePopover;eventPredicate;constructor(t){if(t){let e=Object.keys(t);for(let i of e)t[i]!==void 0&&(this[i]=t[i])}}};var tm=class{connectionPair;scrollableViewProperties;constructor(t,e){this.connectionPair=t,this.scrollableViewProperties=e}};var wE=(()=>{class n{_attachedOverlays=[];_document=u(U);_isAttached=!1;constructor(){}ngOnDestroy(){this.detach()}add(e){this.remove(e),this._attachedOverlays.push(e)}remove(e){let i=this._attachedOverlays.indexOf(e);i>-1&&this._attachedOverlays.splice(i,1),this._attachedOverlays.length===0&&this.detach()}canReceiveEvent(e,i,r){return r.observers.length<1?!1:e.eventPredicate?e.eventPredicate(i):!0}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),CE=(()=>{class n extends wE{_ngZone=u(F);_renderer=u(qe).createRenderer(null,null);_cleanupKeydown;add(e){super.add(e),this._isAttached||(this._ngZone.runOutsideAngular(()=>{this._cleanupKeydown=this._renderer.listen("body","keydown",this._keydownListener)}),this._isAttached=!0)}detach(){this._isAttached&&(this._cleanupKeydown?.(),this._isAttached=!1)}_keydownListener=e=>{let i=this._attachedOverlays;for(let r=i.length-1;r>-1;r--){let o=i[r];if(this.canReceiveEvent(o,e,o._keydownEvents)){this._ngZone.run(()=>o._keydownEvents.next(e));break}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),DE=(()=>{class n extends wE{_platform=u(_e);_ngZone=u(F);_renderer=u(qe).createRenderer(null,null);_cursorOriginalValue;_cursorStyleIsSet=!1;_pointerDownEventTarget=null;_cleanups;add(e){if(super.add(e),!this._isAttached){let i=this._document.body,r={capture:!0},o=this._renderer;this._cleanups=this._ngZone.runOutsideAngular(()=>[o.listen(i,"pointerdown",this._pointerDownListener,r),o.listen(i,"click",this._clickListener,r),o.listen(i,"auxclick",this._clickListener,r),o.listen(i,"contextmenu",this._clickListener,r)]),this._platform.IOS&&!this._cursorStyleIsSet&&(this._cursorOriginalValue=i.style.cursor,i.style.cursor="pointer",this._cursorStyleIsSet=!0),this._isAttached=!0}}detach(){this._isAttached&&(this._cleanups?.forEach(e=>e()),this._cleanups=void 0,this._platform.IOS&&this._cursorStyleIsSet&&(this._document.body.style.cursor=this._cursorOriginalValue,this._cursorStyleIsSet=!1),this._isAttached=!1)}_pointerDownListener=e=>{this._pointerDownEventTarget=ft(e)};_clickListener=e=>{let i=ft(e),r=e.type==="click"&&this._pointerDownEventTarget?this._pointerDownEventTarget:i;this._pointerDownEventTarget=null;let o=this._attachedOverlays.slice();for(let s=o.length-1;s>-1;s--){let a=o[s],l=a._outsidePointerEvents;if(!(!a.hasAttached()||!this.canReceiveEvent(a,e,l))){if(fE(a.overlayElement,i)||fE(a.overlayElement,r))break;this._ngZone?this._ngZone.run(()=>l.next(e)):l.next(e)}}};static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function fE(n,t){let e=typeof ShadowRoot<"u"&&ShadowRoot,i=t;for(;i;){if(i===n)return!0;i=e&&i instanceof ShadowRoot?i.host:i.parentNode}return!1}var xE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["ng-component"]],hostAttrs:["cdk-overlay-style-loader",""],decls:0,vars:0,template:function(i,r){},styles:[`.cdk-overlay-container, .cdk-global-overlay-wrapper {
  pointer-events: none;
  top: 0;
  left: 0;
  height: 100%;
  width: 100%;
}

.cdk-overlay-container {
  position: fixed;
}
@layer cdk-overlay {
  .cdk-overlay-container {
    z-index: 1000;
  }
}
.cdk-overlay-container:empty {
  display: none;
}

.cdk-global-overlay-wrapper {
  display: flex;
  position: absolute;
}
@layer cdk-overlay {
  .cdk-global-overlay-wrapper {
    z-index: 1000;
  }
}

.cdk-overlay-pane {
  position: absolute;
  pointer-events: auto;
  box-sizing: border-box;
  display: flex;
  max-width: 100%;
  max-height: 100%;
}
@layer cdk-overlay {
  .cdk-overlay-pane {
    z-index: 1000;
  }
}

.cdk-overlay-backdrop {
  position: absolute;
  top: 0;
  bottom: 0;
  left: 0;
  right: 0;
  pointer-events: auto;
  -webkit-tap-highlight-color: transparent;
  opacity: 0;
  touch-action: manipulation;
}
@layer cdk-overlay {
  .cdk-overlay-backdrop {
    z-index: 1000;
    transition: opacity 400ms cubic-bezier(0.25, 0.8, 0.25, 1);
  }
}
@media (prefers-reduced-motion) {
  .cdk-overlay-backdrop {
    transition-duration: 1ms;
  }
}

.cdk-overlay-backdrop-showing {
  opacity: 1;
}
@media (forced-colors: active) {
  .cdk-overlay-backdrop-showing {
    opacity: 0.6;
  }
}

@layer cdk-overlay {
  .cdk-overlay-dark-backdrop {
    background: rgba(0, 0, 0, 0.32);
  }
}

.cdk-overlay-transparent-backdrop {
  transition: visibility 1ms linear, opacity 1ms linear;
  visibility: hidden;
  opacity: 1;
}
.cdk-overlay-transparent-backdrop.cdk-overlay-backdrop-showing, .cdk-high-contrast-active .cdk-overlay-transparent-backdrop {
  opacity: 0;
  visibility: visible;
}

.cdk-overlay-backdrop-noop-animation {
  transition: none;
}

.cdk-overlay-connected-position-bounding-box {
  position: absolute;
  display: flex;
  flex-direction: column;
  min-width: 1px;
  min-height: 1px;
}
@layer cdk-overlay {
  .cdk-overlay-connected-position-bounding-box {
    z-index: 1000;
  }
}

.cdk-global-scrollblock {
  position: fixed;
  width: 100%;
  overflow-y: scroll;
}

.cdk-overlay-popover {
  background: none;
  border: none;
  padding: 0;
  outline: 0;
  overflow: visible;
  position: fixed;
  pointer-events: none;
  white-space: normal;
  color: inherit;
  text-decoration: none;
  width: 100%;
  height: 100%;
  inset: auto;
  top: 0;
  left: 0;
}
.cdk-overlay-popover::backdrop {
  display: none;
}
.cdk-overlay-popover .cdk-overlay-backdrop {
  position: fixed;
  z-index: auto;
}
`],encapsulation:2,changeDetection:0})}return n})(),rm=(()=>{class n{_platform=u(_e);_containerElement;_document=u(U);_styleLoader=u(it);constructor(){}ngOnDestroy(){this._containerElement?.remove()}getContainerElement(){return this._loadStyles(),this._containerElement||this._createContainer(),this._containerElement}_createContainer(){let e="cdk-overlay-container";if(this._platform.isBrowser||f_()){let r=this._document.querySelectorAll(`.${e}[platform="server"], .${e}[platform="test"]`);for(let o=0;o<r.length;o++)r[o].remove()}let i=this._document.createElement("div");i.classList.add(e),f_()?i.setAttribute("platform","test"):this._platform.isBrowser||i.setAttribute("platform","server"),this._document.body.appendChild(i),this._containerElement=i}_loadStyles(){this._styleLoader.load(xE)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),A_=class{_renderer;_ngZone;element;_cleanupClick;_cleanupTransitionEnd;_fallbackTimeout;constructor(t,e,i,r){this._renderer=e,this._ngZone=i,this.element=t.createElement("div"),this.element.classList.add("cdk-overlay-backdrop"),this._cleanupClick=e.listen(this.element,"click",r)}detach(){this._ngZone.runOutsideAngular(()=>{let t=this.element;clearTimeout(this._fallbackTimeout),this._cleanupTransitionEnd?.(),this._cleanupTransitionEnd=this._renderer.listen(t,"transitionend",this.dispose),this._fallbackTimeout=setTimeout(this.dispose,500),t.style.pointerEvents="none",t.classList.remove("cdk-overlay-backdrop-showing")})}dispose=()=>{clearTimeout(this._fallbackTimeout),this._cleanupClick?.(),this._cleanupTransitionEnd?.(),this._cleanupClick=this._cleanupTransitionEnd=this._fallbackTimeout=void 0,this.element.remove()}};function R_(n){return n&&n.nodeType===1}var ms=class{_portalOutlet;_host;_pane;_config;_ngZone;_keyboardDispatcher;_document;_location;_outsideClickDispatcher;_animationsDisabled;_injector;_renderer;_backdropClick=new k;_attachments=new k;_detachments=new k;_positionStrategy;_scrollStrategy;_locationChanges=ge.EMPTY;_backdropRef=null;_detachContentMutationObserver;_detachContentAfterRenderRef;_disposed=!1;_previousHostParent;_keydownEvents=new k;_outsidePointerEvents=new k;_afterNextRenderRef;constructor(t,e,i,r,o,s,a,l,c,d=!1,m,f){this._portalOutlet=t,this._host=e,this._pane=i,this._config=r,this._ngZone=o,this._keyboardDispatcher=s,this._document=a,this._location=l,this._outsideClickDispatcher=c,this._animationsDisabled=d,this._injector=m,this._renderer=f,r.scrollStrategy&&(this._scrollStrategy=r.scrollStrategy,this._scrollStrategy.attach(this)),this._positionStrategy=r.positionStrategy}get overlayElement(){return this._pane}get backdropElement(){return this._backdropRef?.element||null}get hostElement(){return this._host}get eventPredicate(){return this._config?.eventPredicate||null}attach(t){if(this._disposed)return null;this._attachHost();let e=this._portalOutlet.attach(t);return this._positionStrategy?.attach(this),this._updateStackingOrder(),this._updateElementSize(),this._updateElementDirection(),this._scrollStrategy&&this._scrollStrategy.enable(),this._afterNextRenderRef?.destroy(),this._afterNextRenderRef=Qe(()=>{this.hasAttached()&&this.updatePosition()},{injector:this._injector}),this._togglePointerEvents(!0),this._config.hasBackdrop&&this._attachBackdrop(),this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!0),this._attachments.next(),this._completeDetachContent(),this._keyboardDispatcher.add(this),this._config.disposeOnNavigation&&(this._locationChanges=this._location.subscribe(()=>this.dispose())),this._outsideClickDispatcher.add(this),typeof e?.onDestroy=="function"&&e.onDestroy(()=>{this.hasAttached()&&this._ngZone.runOutsideAngular(()=>Promise.resolve().then(()=>this.detach()))}),e}detach(){if(!this.hasAttached())return;this.detachBackdrop(),this._togglePointerEvents(!1),this._positionStrategy&&this._positionStrategy.detach&&this._positionStrategy.detach(),this._scrollStrategy&&this._scrollStrategy.disable();let t=this._portalOutlet.detach();return this._detachments.next(),this._completeDetachContent(),this._keyboardDispatcher.remove(this),this._detachContentWhenEmpty(),this._locationChanges.unsubscribe(),this._outsideClickDispatcher.remove(this),t}dispose(){if(this._disposed)return;let t=this.hasAttached();this._positionStrategy&&this._positionStrategy.dispose(),this._disposeScrollStrategy(),this._backdropRef?.dispose(),this._locationChanges.unsubscribe(),this._keyboardDispatcher.remove(this),this._portalOutlet.dispose(),this._attachments.complete(),this._backdropClick.complete(),this._keydownEvents.complete(),this._outsidePointerEvents.complete(),this._outsideClickDispatcher.remove(this),this._host?.remove(),this._afterNextRenderRef?.destroy(),this._previousHostParent=this._pane=this._host=this._backdropRef=null,t&&this._detachments.next(),this._detachments.complete(),this._completeDetachContent(),this._disposed=!0}hasAttached(){return this._portalOutlet.hasAttached()}backdropClick(){return this._backdropClick}attachments(){return this._attachments}detachments(){return this._detachments}keydownEvents(){return this._keydownEvents}outsidePointerEvents(){return this._outsidePointerEvents}getConfig(){return this._config}updatePosition(){this._positionStrategy&&this._positionStrategy.apply()}updatePositionStrategy(t){t!==this._positionStrategy&&(this._positionStrategy&&this._positionStrategy.dispose(),this._positionStrategy=t,this.hasAttached()&&(t.attach(this),this.updatePosition()))}updateSize(t){this._config=M(M({},this._config),t),this._updateElementSize()}setDirection(t){this._config=be(M({},this._config),{direction:t}),this._updateElementDirection()}addPanelClass(t){this._pane&&this._toggleClasses(this._pane,t,!0)}removePanelClass(t){this._pane&&this._toggleClasses(this._pane,t,!1)}getDirection(){let t=this._config.direction;return t?typeof t=="string"?t:t.value:"ltr"}updateScrollStrategy(t){t!==this._scrollStrategy&&(this._disposeScrollStrategy(),this._scrollStrategy=t,this.hasAttached()&&(t.attach(this),t.enable()))}_updateElementDirection(){this._host.setAttribute("dir",this.getDirection())}_updateElementSize(){if(!this._pane)return;let t=this._pane.style;t.width=et(this._config.width),t.height=et(this._config.height),t.minWidth=et(this._config.minWidth),t.minHeight=et(this._config.minHeight),t.maxWidth=et(this._config.maxWidth),t.maxHeight=et(this._config.maxHeight)}_togglePointerEvents(t){this._pane.style.pointerEvents=t?"":"none"}_attachHost(){if(!this._host.parentElement){let t=this._config.usePopover?this._positionStrategy?.getPopoverInsertionPoint?.():null;R_(t)?t.after(this._host):t?.type==="parent"?t.element.appendChild(this._host):this._previousHostParent?.appendChild(this._host)}if(this._config.usePopover)try{this._host.showPopover()}catch(t){}}_attachBackdrop(){let t="cdk-overlay-backdrop-showing";this._backdropRef?.dispose(),this._backdropRef=new A_(this._document,this._renderer,this._ngZone,e=>{this._backdropClick.next(e)}),this._animationsDisabled&&this._backdropRef.element.classList.add("cdk-overlay-backdrop-noop-animation"),this._config.backdropClass&&this._toggleClasses(this._backdropRef.element,this._config.backdropClass,!0),this._config.usePopover?this._host.prepend(this._backdropRef.element):this._host.parentElement.insertBefore(this._backdropRef.element,this._host),!this._animationsDisabled&&typeof requestAnimationFrame<"u"?this._ngZone.runOutsideAngular(()=>{requestAnimationFrame(()=>this._backdropRef?.element.classList.add(t))}):this._backdropRef.element.classList.add(t)}_updateStackingOrder(){!this._config.usePopover&&this._host.nextSibling&&this._host.parentNode.appendChild(this._host)}detachBackdrop(){this._animationsDisabled?(this._backdropRef?.dispose(),this._backdropRef=null):this._backdropRef?.detach()}_toggleClasses(t,e,i){let r=rs(e||[]).filter(o=>!!o);r.length&&(i?t.classList.add(...r):t.classList.remove(...r))}_detachContentWhenEmpty(){let t=!1;try{this._detachContentAfterRenderRef=Qe(()=>{t=!0,this._detachContent()},{injector:this._injector})}catch(e){if(t)throw e;this._detachContent()}globalThis.MutationObserver&&this._pane&&(this._detachContentMutationObserver||=new globalThis.MutationObserver(()=>{this._detachContent()}),this._detachContentMutationObserver.observe(this._pane,{childList:!0}))}_detachContent(){(!this._pane||!this._host||this._pane.children.length===0)&&(this._pane&&this._config.panelClass&&this._toggleClasses(this._pane,this._config.panelClass,!1),this._host&&this._host.parentElement&&(this._previousHostParent=this._host.parentElement,this._host.remove()),this._completeDetachContent())}_completeDetachContent(){this._detachContentAfterRenderRef?.destroy(),this._detachContentAfterRenderRef=void 0,this._detachContentMutationObserver?.disconnect()}_disposeScrollStrategy(){let t=this._scrollStrategy;t?.disable(),t?.detach?.()}},pE="cdk-overlay-connected-position-bounding-box",YN=/([A-Za-z%]+)$/;function ml(n,t){return new nm(t,n.get(Sn),n.get(U),n.get(_e),n.get(rm))}var nm=class{_viewportRuler;_document;_platform;_overlayContainer;_overlayRef;_isInitialRender=!1;_lastBoundingBoxSize={width:0,height:0};_isPushed=!1;_canPush=!0;_growAfterOpen=!1;_hasFlexibleDimensions=!0;_positionLocked=!1;_originRect;_overlayRect;_viewportRect;_containerRect;_viewportMargin=0;_scrollables=[];_preferredPositions=[];_origin;_pane;_isDisposed=!1;_boundingBox=null;_lastPosition=null;_lastScrollVisibility=null;_positionChanges=new k;_resizeSubscription=ge.EMPTY;_offsetX=0;_offsetY=0;_transformOriginSelector;_appliedPanelClasses=[];_previousPushAmount=null;_popoverLocation="global";positionChanges=this._positionChanges;get positions(){return this._preferredPositions}constructor(t,e,i,r,o){this._viewportRuler=e,this._document=i,this._platform=r,this._overlayContainer=o,this.setOrigin(t)}attach(t){this._overlayRef&&this._overlayRef,this._validatePositions(),t.hostElement.classList.add(pE),this._overlayRef=t,this._boundingBox=t.hostElement,this._pane=t.overlayElement,this._isDisposed=!1,this._isInitialRender=!0,this._lastPosition=null,this._resizeSubscription.unsubscribe(),this._resizeSubscription=this._viewportRuler.change().subscribe(()=>{this._isInitialRender=!0,this.apply()})}apply(){if(this._isDisposed||!this._platform.isBrowser)return;if(!this._isInitialRender&&this._positionLocked&&this._lastPosition){this.reapplyLastPosition();return}this._clearPanelClasses(),this._resetOverlayElementStyles(),this._resetBoundingBoxStyles(),this._viewportRect=this._getNarrowedViewportRect(),this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._containerRect=this._getContainerRect();let t=this._originRect,e=this._overlayRect,i=this._viewportRect,r=this._containerRect,o=[],s;for(let a of this._preferredPositions){let l=this._getOriginPoint(t,r,a),c=this._getOverlayPoint(l,e,a),d=this._getOverlayFit(c,e,i,a);if(d.isCompletelyWithinViewport){this._isPushed=!1,this._applyPosition(a,l);return}if(this._canFitWithFlexibleDimensions(d,c,i)){o.push({position:a,origin:l,overlayRect:e,boundingBoxRect:this._calculateBoundingBoxRect(l,a)});continue}(!s||s.overlayFit.visibleArea<d.visibleArea)&&(s={overlayFit:d,overlayPoint:c,originPoint:l,position:a,overlayRect:e})}if(o.length){let a=null,l=-1;for(let c of o){let d=c.boundingBoxRect.width*c.boundingBoxRect.height*(c.position.weight||1);d>l&&(l=d,a=c)}this._isPushed=!1,this._applyPosition(a.position,a.origin);return}if(this._canPush){this._isPushed=!0,this._applyPosition(s.position,s.originPoint);return}this._applyPosition(s.position,s.originPoint)}detach(){this._clearPanelClasses(),this._lastPosition=null,this._previousPushAmount=null,this._resizeSubscription.unsubscribe()}dispose(){this._isDisposed||(this._boundingBox&&Zr(this._boundingBox.style,{top:"",left:"",right:"",bottom:"",height:"",width:"",alignItems:"",justifyContent:""}),this._pane&&this._resetOverlayElementStyles(),this._overlayRef&&this._overlayRef.hostElement.classList.remove(pE),this.detach(),this._positionChanges.complete(),this._overlayRef=this._boundingBox=null,this._isDisposed=!0)}reapplyLastPosition(){if(this._isDisposed||!this._platform.isBrowser)return;let t=this._lastPosition;t?(this._originRect=this._getOriginRect(),this._overlayRect=this._pane.getBoundingClientRect(),this._viewportRect=this._getNarrowedViewportRect(),this._containerRect=this._getContainerRect(),this._applyPosition(t,this._getOriginPoint(this._originRect,this._containerRect,t))):this.apply()}withScrollableContainers(t){return this._scrollables=t,this}withPositions(t){return this._preferredPositions=t,t.indexOf(this._lastPosition)===-1&&(this._lastPosition=null),this._validatePositions(),this}withViewportMargin(t){return this._viewportMargin=t,this}withFlexibleDimensions(t=!0){return this._hasFlexibleDimensions=t,this}withGrowAfterOpen(t=!0){return this._growAfterOpen=t,this}withPush(t=!0){return this._canPush=t,this}withLockedPosition(t=!0){return this._positionLocked=t,this}setOrigin(t){return this._origin=t,this}withDefaultOffsetX(t){return this._offsetX=t,this}withDefaultOffsetY(t){return this._offsetY=t,this}withTransformOriginOn(t){return this._transformOriginSelector=t,this}withPopoverLocation(t){return this._popoverLocation=t,this}getPopoverInsertionPoint(){return this._popoverLocation==="global"?null:this._popoverLocation!=="inline"?this._popoverLocation:this._origin instanceof P?this._origin.nativeElement:R_(this._origin)?this._origin:null}_getOriginPoint(t,e,i){let r;if(i.originX=="center")r=t.left+t.width/2;else{let s=this._isRtl()?t.right:t.left,a=this._isRtl()?t.left:t.right;r=i.originX=="start"?s:a}e.left<0&&(r-=e.left);let o;return i.originY=="center"?o=t.top+t.height/2:o=i.originY=="top"?t.top:t.bottom,e.top<0&&(o-=e.top),{x:r,y:o}}_getOverlayPoint(t,e,i){let r;i.overlayX=="center"?r=-e.width/2:i.overlayX==="start"?r=this._isRtl()?-e.width:0:r=this._isRtl()?0:-e.width;let o;return i.overlayY=="center"?o=-e.height/2:o=i.overlayY=="top"?0:-e.height,{x:t.x+r,y:t.y+o}}_getOverlayFit(t,e,i,r){let o=_E(e),{x:s,y:a}=t,l=this._getOffset(r,"x"),c=this._getOffset(r,"y");l&&(s+=l),c&&(a+=c);let d=0-s,m=s+o.width-i.width,f=0-a,h=a+o.height-i.height,p=this._subtractOverflows(o.width,d,m),b=this._subtractOverflows(o.height,f,h),C=p*b;return{visibleArea:C,isCompletelyWithinViewport:o.width*o.height===C,fitsInViewportVertically:b===o.height,fitsInViewportHorizontally:p==o.width}}_canFitWithFlexibleDimensions(t,e,i){if(this._hasFlexibleDimensions){let r=i.bottom-e.y,o=i.right-e.x,s=gE(this._overlayRef.getConfig().minHeight),a=gE(this._overlayRef.getConfig().minWidth),l=t.fitsInViewportVertically||s!=null&&s<=r,c=t.fitsInViewportHorizontally||a!=null&&a<=o;return l&&c}return!1}_pushOverlayOnScreen(t,e,i){if(this._previousPushAmount&&this._positionLocked)return{x:t.x+this._previousPushAmount.x,y:t.y+this._previousPushAmount.y};let r=_E(e),o=this._viewportRect,s=Math.max(t.x+r.width-o.width,0),a=Math.max(t.y+r.height-o.height,0),l=Math.max(o.top-i.top-t.y,0),c=Math.max(o.left-i.left-t.x,0),d=0,m=0;return r.width<=o.width?d=c||-s:d=t.x<this._getViewportMarginStart()?o.left-i.left-t.x:0,r.height<=o.height?m=l||-a:m=t.y<this._getViewportMarginTop()?o.top-i.top-t.y:0,this._previousPushAmount={x:d,y:m},{x:t.x+d,y:t.y+m}}_applyPosition(t,e){if(this._setTransformOrigin(t),this._setOverlayElementStyles(e,t),this._setBoundingBoxStyles(e,t),t.panelClass&&this._addPanelClasses(t.panelClass),this._positionChanges.observers.length){let i=this._getScrollVisibility();if(t!==this._lastPosition||!this._lastScrollVisibility||!ZN(this._lastScrollVisibility,i)){let r=new tm(t,i);this._positionChanges.next(r)}this._lastScrollVisibility=i}this._lastPosition=t,this._isInitialRender=!1}_setTransformOrigin(t){if(!this._transformOriginSelector)return;let e=this._boundingBox.querySelectorAll(this._transformOriginSelector),i,r=t.overlayY;t.overlayX==="center"?i="center":this._isRtl()?i=t.overlayX==="start"?"right":"left":i=t.overlayX==="start"?"left":"right";for(let o=0;o<e.length;o++)e[o].style.transformOrigin=`${i} ${r}`}_calculateBoundingBoxRect(t,e){let i=this._viewportRect,r=this._isRtl(),o,s,a;if(e.overlayY==="top")s=t.y,o=i.height-s+this._getViewportMarginBottom();else if(e.overlayY==="bottom")a=i.height-t.y+this._getViewportMarginTop()+this._getViewportMarginBottom(),o=i.height-a+this._getViewportMarginTop();else{let h=Math.min(i.bottom-t.y+i.top,t.y),p=this._lastBoundingBoxSize.height;o=h*2,s=t.y-h,o>p&&!this._isInitialRender&&!this._growAfterOpen&&(s=t.y-p/2)}let l=e.overlayX==="start"&&!r||e.overlayX==="end"&&r,c=e.overlayX==="end"&&!r||e.overlayX==="start"&&r,d,m,f;if(c)f=i.width-t.x+this._getViewportMarginStart()+this._getViewportMarginEnd(),d=t.x-this._getViewportMarginStart();else if(l)m=t.x,d=i.right-t.x-this._getViewportMarginEnd();else{let h=Math.min(i.right-t.x+i.left,t.x),p=this._lastBoundingBoxSize.width;d=h*2,m=t.x-h,d>p&&!this._isInitialRender&&!this._growAfterOpen&&(m=t.x-p/2)}return{top:s,left:m,bottom:a,right:f,width:d,height:o}}_setBoundingBoxStyles(t,e){let i=this._calculateBoundingBoxRect(t,e);!this._isInitialRender&&!this._growAfterOpen&&(i.height=Math.min(i.height,this._lastBoundingBoxSize.height),i.width=Math.min(i.width,this._lastBoundingBoxSize.width));let r={};if(this._hasExactPosition())r.top=r.left="0",r.bottom=r.right="auto",r.maxHeight=r.maxWidth="",r.width=r.height="100%";else{let o=this._overlayRef.getConfig().maxHeight,s=this._overlayRef.getConfig().maxWidth;r.width=et(i.width),r.height=et(i.height),r.top=et(i.top)||"auto",r.bottom=et(i.bottom)||"auto",r.left=et(i.left)||"auto",r.right=et(i.right)||"auto",e.overlayX==="center"?r.alignItems="center":r.alignItems=e.overlayX==="end"?"flex-end":"flex-start",e.overlayY==="center"?r.justifyContent="center":r.justifyContent=e.overlayY==="bottom"?"flex-end":"flex-start",o&&(r.maxHeight=et(o)),s&&(r.maxWidth=et(s))}this._lastBoundingBoxSize=i,Zr(this._boundingBox.style,r)}_resetBoundingBoxStyles(){Zr(this._boundingBox.style,{top:"0",left:"0",right:"0",bottom:"0",height:"",width:"",alignItems:"",justifyContent:""})}_resetOverlayElementStyles(){Zr(this._pane.style,{top:"",left:"",bottom:"",right:"",position:"",transform:""})}_setOverlayElementStyles(t,e){let i={},r=this._hasExactPosition(),o=this._hasFlexibleDimensions,s=this._overlayRef.getConfig();if(r){let d=this._viewportRuler.getViewportScrollPosition();Zr(i,this._getExactOverlayY(e,t,d)),Zr(i,this._getExactOverlayX(e,t,d))}else i.position="static";let a="",l=this._getOffset(e,"x"),c=this._getOffset(e,"y");l&&(a+=`translateX(${l}px) `),c&&(a+=`translateY(${c}px)`),i.transform=a.trim(),s.maxHeight&&(r?i.maxHeight=et(s.maxHeight):o&&(i.maxHeight="")),s.maxWidth&&(r?i.maxWidth=et(s.maxWidth):o&&(i.maxWidth="")),Zr(this._pane.style,i)}_getExactOverlayY(t,e,i){let r={top:"",bottom:""},o=this._getOverlayPoint(e,this._overlayRect,t);if(this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i)),t.overlayY==="bottom"){let s=this._document.documentElement.clientHeight;r.bottom=`${s-(o.y+this._overlayRect.height)}px`}else r.top=et(o.y);return r}_getExactOverlayX(t,e,i){let r={left:"",right:""},o=this._getOverlayPoint(e,this._overlayRect,t);this._isPushed&&(o=this._pushOverlayOnScreen(o,this._overlayRect,i));let s;if(this._isRtl()?s=t.overlayX==="end"?"left":"right":s=t.overlayX==="end"?"right":"left",s==="right"){let a=this._document.documentElement.clientWidth;r.right=`${a-(o.x+this._overlayRect.width)}px`}else r.left=et(o.x);return r}_getScrollVisibility(){let t=this._getOriginRect(),e=this._pane.getBoundingClientRect(),i=this._scrollables.map(r=>r.getElementRef().nativeElement.getBoundingClientRect());return{isOriginClipped:hE(t,i),isOriginOutsideView:T_(t,i),isOverlayClipped:hE(e,i),isOverlayOutsideView:T_(e,i)}}_subtractOverflows(t,...e){return e.reduce((i,r)=>i-Math.max(r,0),t)}_getNarrowedViewportRect(){let t=this._document.documentElement.clientWidth,e=this._document.documentElement.clientHeight,i=this._viewportRuler.getViewportScrollPosition();return{top:i.top+this._getViewportMarginTop(),left:i.left+this._getViewportMarginStart(),right:i.left+t-this._getViewportMarginEnd(),bottom:i.top+e-this._getViewportMarginBottom(),width:t-this._getViewportMarginStart()-this._getViewportMarginEnd(),height:e-this._getViewportMarginTop()-this._getViewportMarginBottom()}}_isRtl(){return this._overlayRef.getDirection()==="rtl"}_hasExactPosition(){return!this._hasFlexibleDimensions||this._isPushed}_getOffset(t,e){return e==="x"?t.offsetX==null?this._offsetX:t.offsetX:t.offsetY==null?this._offsetY:t.offsetY}_validatePositions(){}_addPanelClasses(t){this._pane&&rs(t).forEach(e=>{e!==""&&this._appliedPanelClasses.indexOf(e)===-1&&(this._appliedPanelClasses.push(e),this._pane.classList.add(e))})}_clearPanelClasses(){this._pane&&(this._appliedPanelClasses.forEach(t=>{this._pane.classList.remove(t)}),this._appliedPanelClasses=[])}_getViewportMarginStart(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.start??0}_getViewportMarginEnd(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.end??0}_getViewportMarginTop(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.top??0}_getViewportMarginBottom(){return typeof this._viewportMargin=="number"?this._viewportMargin:this._viewportMargin?.bottom??0}_getOriginRect(){let t=this._origin;if(t instanceof P)return t.nativeElement.getBoundingClientRect();if(t instanceof Element)return t.getBoundingClientRect();let e=t.width||0,i=t.height||0;return{top:t.y,bottom:t.y+i,left:t.x,right:t.x+e,height:i,width:e}}_getContainerRect(){let t=this._overlayRef.getConfig().usePopover&&this._popoverLocation!=="global",e=this._overlayContainer.getContainerElement();t&&(e.style.display="block");let i=e.getBoundingClientRect();return t&&(e.style.display=""),i}};function Zr(n,t){for(let e in t)t.hasOwnProperty(e)&&(n[e]=t[e]);return n}function gE(n){if(typeof n!="number"&&n!=null){let[t,e]=n.split(YN);return!e||e==="px"?parseFloat(t):null}return n||null}function _E(n){return{top:Math.floor(n.top),right:Math.floor(n.right),bottom:Math.floor(n.bottom),left:Math.floor(n.left),width:Math.floor(n.width),height:Math.floor(n.height)}}function ZN(n,t){return n===t?!0:n.isOriginClipped===t.isOriginClipped&&n.isOriginOutsideView===t.isOriginOutsideView&&n.isOverlayClipped===t.isOverlayClipped&&n.isOverlayOutsideView===t.isOverlayOutsideView}var vE="cdk-global-overlay-wrapper";function er(n){return new im}var im=class{_overlayRef;_cssPosition="static";_topOffset="";_bottomOffset="";_alignItems="";_xPosition="";_xOffset="";_width="";_height="";_isDisposed=!1;attach(t){let e=t.getConfig();this._overlayRef=t,this._width&&!e.width&&t.updateSize({width:this._width}),this._height&&!e.height&&t.updateSize({height:this._height}),t.hostElement.classList.add(vE),this._isDisposed=!1}top(t=""){return this._bottomOffset="",this._topOffset=t,this._alignItems="flex-start",this}left(t=""){return this._xOffset=t,this._xPosition="left",this}bottom(t=""){return this._topOffset="",this._bottomOffset=t,this._alignItems="flex-end",this}right(t=""){return this._xOffset=t,this._xPosition="right",this}start(t=""){return this._xOffset=t,this._xPosition="start",this}end(t=""){return this._xOffset=t,this._xPosition="end",this}width(t=""){return this._overlayRef?this._overlayRef.updateSize({width:t}):this._width=t,this}height(t=""){return this._overlayRef?this._overlayRef.updateSize({height:t}):this._height=t,this}centerHorizontally(t=""){return this.left(t),this._xPosition="center",this}centerVertically(t=""){return this.top(t),this._alignItems="center",this}apply(){if(!this._overlayRef||!this._overlayRef.hasAttached())return;let t=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement.style,i=this._overlayRef.getConfig(),{width:r,height:o,maxWidth:s,maxHeight:a}=i,l=(r==="100%"||r==="100vw")&&(!s||s==="100%"||s==="100vw"),c=(o==="100%"||o==="100vh")&&(!a||a==="100%"||a==="100vh"),d=this._xPosition,m=this._xOffset,f=this._overlayRef.getConfig().direction==="rtl",h="",p="",b="";l?b="flex-start":d==="center"?(b="center",f?p=m:h=m):f?d==="left"||d==="end"?(b="flex-end",h=m):(d==="right"||d==="start")&&(b="flex-start",p=m):d==="left"||d==="start"?(b="flex-start",h=m):(d==="right"||d==="end")&&(b="flex-end",p=m),t.position=this._cssPosition,t.marginLeft=l?"0":h,t.marginTop=c?"0":this._topOffset,t.marginBottom=this._bottomOffset,t.marginRight=l?"0":p,e.justifyContent=b,e.alignItems=c?"flex-start":this._alignItems}dispose(){if(this._isDisposed||!this._overlayRef)return;let t=this._overlayRef.overlayElement.style,e=this._overlayRef.hostElement,i=e.style;e.classList.remove(vE),i.justifyContent=i.alignItems=t.marginTop=t.marginBottom=t.marginLeft=t.marginRight=t.position="",this._overlayRef=null,this._isDisposed=!0}},EE=(()=>{class n{_injector=u($);constructor(){}global(){return er()}flexibleConnectedTo(e){return ml(this._injector,e)}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),hl=new v("OVERLAY_DEFAULT_CONFIG");function vi(n,t){n.get(it).load(xE);let e=n.get(rm),i=n.get(U),r=n.get(Ie),o=n.get(Ut),s=n.get(st),a=n.get(Ke,null,{optional:!0})||n.get(qe).createRenderer(null,null),l=new _i(t),c=n.get(hl,null,{optional:!0})?.usePopover??!0;l.direction=l.direction||s.value,"showPopover"in i.body?l.usePopover=t?.usePopover??c:l.usePopover=!1;let d=i.createElement("div"),m=i.createElement("div");d.id=r.getId("cdk-overlay-"),d.classList.add("cdk-overlay-pane"),m.appendChild(d),l.usePopover&&(m.setAttribute("popover","manual"),m.classList.add("cdk-overlay-popover"));let f=l.usePopover?l.positionStrategy?.getPopoverInsertionPoint?.():null;return R_(f)?f.after(m):f?.type==="parent"?f.element.appendChild(m):e.getContainerElement().appendChild(m),new ms(new Zu(d,o,n),m,d,l,n.get(F),n.get(CE),i,n.get(jd),n.get(DE),t?.disableAnimations??n.get(Vi,null,{optional:!0})==="NoopAnimations",n.get(Ze),a)}var kE=(()=>{class n{scrollStrategies=u(yE);_positionBuilder=u(EE);_injector=u($);constructor(){}create(e){return vi(this._injector,e)}position(){return this._positionBuilder}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),XN=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"}],JN=new v("cdk-connected-overlay-scroll-strategy",{providedIn:"root",factory:()=>{let n=u($);return()=>Jr(n)}}),hs=(()=>{class n{elementRef=u(P);constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdk-overlay-origin",""],["","overlay-origin",""],["","cdkOverlayOrigin",""]],exportAs:["cdkOverlayOrigin"]})}return n})(),SE=new v("cdk-connected-overlay-default-config"),om=(()=>{class n{_dir=u(st,{optional:!0});_injector=u($);_overlayRef;_templatePortal;_backdropSubscription=ge.EMPTY;_attachSubscription=ge.EMPTY;_detachSubscription=ge.EMPTY;_positionSubscription=ge.EMPTY;_offsetX;_offsetY;_position;_scrollStrategyFactory=u(JN);_ngZone=u(F);origin;positions;positionStrategy;get offsetX(){return this._offsetX}set offsetX(e){this._offsetX=e,this._position&&this._updatePositionStrategy(this._position)}get offsetY(){return this._offsetY}set offsetY(e){this._offsetY=e,this._position&&this._updatePositionStrategy(this._position)}width;height;minWidth;minHeight;backdropClass;panelClass;viewportMargin=0;scrollStrategy;open=!1;disableClose=!1;transformOriginSelector;hasBackdrop=!1;lockPosition=!1;flexibleDimensions=!1;growAfterOpen=!1;push=!1;disposeOnNavigation=!1;usePopover;matchWidth=!1;set _config(e){typeof e!="string"&&this._assignConfig(e)}backdropClick=new z;positionChange=new z;attach=new z;detach=new z;overlayKeydown=new z;overlayOutsideClick=new z;constructor(){let e=u(Pe),i=u(tt),r=u(SE,{optional:!0}),o=u(hl,{optional:!0});this.usePopover=o?.usePopover===!1?null:"global",this._templatePortal=new gi(e,i),this.scrollStrategy=this._scrollStrategyFactory(),r&&this._assignConfig(r)}get overlayRef(){return this._overlayRef}get dir(){return this._dir?this._dir.value:"ltr"}ngOnDestroy(){this._attachSubscription.unsubscribe(),this._detachSubscription.unsubscribe(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this._overlayRef?.dispose()}ngOnChanges(e){this._position&&(this._updatePositionStrategy(this._position),this._overlayRef?.updateSize({width:this._getWidth(),minWidth:this.minWidth,height:this.height,minHeight:this.minHeight}),e.origin&&this.open&&this._position.apply()),e.open&&(this.open?this.attachOverlay():this.detachOverlay())}_createOverlay(){(!this.positions||!this.positions.length)&&(this.positions=XN);let e=this._overlayRef=vi(this._injector,this._buildConfig());this._attachSubscription=e.attachments().subscribe(()=>this.attach.emit()),this._detachSubscription=e.detachments().subscribe(()=>this.detach.emit()),e.keydownEvents().subscribe(i=>{this.overlayKeydown.next(i),i.keyCode===27&&!this.disableClose&&!ut(i)&&(i.preventDefault(),this.detachOverlay())}),this._overlayRef.outsidePointerEvents().subscribe(i=>{let r=this._getOriginElement(),o=ft(i);(!r||r!==o&&!r.contains(o))&&this.overlayOutsideClick.next(i)})}_buildConfig(){let e=this._position=this.positionStrategy||this._createPositionStrategy(),i=new _i({direction:this._dir||"ltr",positionStrategy:e,scrollStrategy:this.scrollStrategy,hasBackdrop:this.hasBackdrop,disposeOnNavigation:this.disposeOnNavigation,usePopover:!!this.usePopover});return(this.height||this.height===0)&&(i.height=this.height),(this.minWidth||this.minWidth===0)&&(i.minWidth=this.minWidth),(this.minHeight||this.minHeight===0)&&(i.minHeight=this.minHeight),this.backdropClass&&(i.backdropClass=this.backdropClass),this.panelClass&&(i.panelClass=this.panelClass),i}_updatePositionStrategy(e){let i=this.positions.map(r=>({originX:r.originX,originY:r.originY,overlayX:r.overlayX,overlayY:r.overlayY,offsetX:r.offsetX||this.offsetX,offsetY:r.offsetY||this.offsetY,panelClass:r.panelClass||void 0}));return e.setOrigin(this._getOrigin()).withPositions(i).withFlexibleDimensions(this.flexibleDimensions).withPush(this.push).withGrowAfterOpen(this.growAfterOpen).withViewportMargin(this.viewportMargin).withLockedPosition(this.lockPosition).withTransformOriginOn(this.transformOriginSelector).withPopoverLocation(this.usePopover===null?"global":this.usePopover)}_createPositionStrategy(){let e=ml(this._injector,this._getOrigin());return this._updatePositionStrategy(e),e}_getOrigin(){return this.origin instanceof hs?this.origin.elementRef:this.origin}_getOriginElement(){return this.origin instanceof hs?this.origin.elementRef.nativeElement:this.origin instanceof P?this.origin.nativeElement:typeof Element<"u"&&this.origin instanceof Element?this.origin:null}_getWidth(){return this.width?this.width:this.matchWidth?this._getOriginElement()?.getBoundingClientRect?.().width:void 0}attachOverlay(){this._overlayRef||this._createOverlay();let e=this._overlayRef;e.getConfig().hasBackdrop=this.hasBackdrop,e.updateSize({width:this._getWidth()}),e.hasAttached()||e.attach(this._templatePortal),this.hasBackdrop?this._backdropSubscription=e.backdropClick().subscribe(i=>this.backdropClick.emit(i)):this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.positionChange.observers.length>0&&(this._positionSubscription=this._position.positionChanges.pipe(eh(()=>this.positionChange.observers.length>0)).subscribe(i=>{this._ngZone.run(()=>this.positionChange.emit(i)),this.positionChange.observers.length===0&&this._positionSubscription.unsubscribe()})),this.open=!0}detachOverlay(){this._overlayRef?.detach(),this._backdropSubscription.unsubscribe(),this._positionSubscription.unsubscribe(),this.open=!1}_assignConfig(e){this.origin=e.origin??this.origin,this.positions=e.positions??this.positions,this.positionStrategy=e.positionStrategy??this.positionStrategy,this.offsetX=e.offsetX??this.offsetX,this.offsetY=e.offsetY??this.offsetY,this.width=e.width??this.width,this.height=e.height??this.height,this.minWidth=e.minWidth??this.minWidth,this.minHeight=e.minHeight??this.minHeight,this.backdropClass=e.backdropClass??this.backdropClass,this.panelClass=e.panelClass??this.panelClass,this.viewportMargin=e.viewportMargin??this.viewportMargin,this.scrollStrategy=e.scrollStrategy??this.scrollStrategy,this.disableClose=e.disableClose??this.disableClose,this.transformOriginSelector=e.transformOriginSelector??this.transformOriginSelector,this.hasBackdrop=e.hasBackdrop??this.hasBackdrop,this.lockPosition=e.lockPosition??this.lockPosition,this.flexibleDimensions=e.flexibleDimensions??this.flexibleDimensions,this.growAfterOpen=e.growAfterOpen??this.growAfterOpen,this.push=e.push??this.push,this.disposeOnNavigation=e.disposeOnNavigation??this.disposeOnNavigation,this.usePopover=e.usePopover??this.usePopover,this.matchWidth=e.matchWidth??this.matchWidth}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdk-connected-overlay",""],["","connected-overlay",""],["","cdkConnectedOverlay",""]],inputs:{origin:[0,"cdkConnectedOverlayOrigin","origin"],positions:[0,"cdkConnectedOverlayPositions","positions"],positionStrategy:[0,"cdkConnectedOverlayPositionStrategy","positionStrategy"],offsetX:[0,"cdkConnectedOverlayOffsetX","offsetX"],offsetY:[0,"cdkConnectedOverlayOffsetY","offsetY"],width:[0,"cdkConnectedOverlayWidth","width"],height:[0,"cdkConnectedOverlayHeight","height"],minWidth:[0,"cdkConnectedOverlayMinWidth","minWidth"],minHeight:[0,"cdkConnectedOverlayMinHeight","minHeight"],backdropClass:[0,"cdkConnectedOverlayBackdropClass","backdropClass"],panelClass:[0,"cdkConnectedOverlayPanelClass","panelClass"],viewportMargin:[0,"cdkConnectedOverlayViewportMargin","viewportMargin"],scrollStrategy:[0,"cdkConnectedOverlayScrollStrategy","scrollStrategy"],open:[0,"cdkConnectedOverlayOpen","open"],disableClose:[0,"cdkConnectedOverlayDisableClose","disableClose"],transformOriginSelector:[0,"cdkConnectedOverlayTransformOriginOn","transformOriginSelector"],hasBackdrop:[2,"cdkConnectedOverlayHasBackdrop","hasBackdrop",N],lockPosition:[2,"cdkConnectedOverlayLockPosition","lockPosition",N],flexibleDimensions:[2,"cdkConnectedOverlayFlexibleDimensions","flexibleDimensions",N],growAfterOpen:[2,"cdkConnectedOverlayGrowAfterOpen","growAfterOpen",N],push:[2,"cdkConnectedOverlayPush","push",N],disposeOnNavigation:[2,"cdkConnectedOverlayDisposeOnNavigation","disposeOnNavigation",N],usePopover:[0,"cdkConnectedOverlayUsePopover","usePopover"],matchWidth:[2,"cdkConnectedOverlayMatchWidth","matchWidth",N],_config:[0,"cdkConnectedOverlay","_config"]},outputs:{backdropClick:"backdropClick",positionChange:"positionChange",attach:"attach",detach:"detach",overlayKeydown:"overlayKeydown",overlayOutsideClick:"overlayOutsideClick"},exportAs:["cdkConnectedOverlay"],features:[Le]})}return n})(),Qn=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[kE],imports:[oe,Ji,cl,cl]})}return n})();var sm=(()=>{class n{_animationsDisabled=Fe();state="unchecked";disabled=!1;appearance="full";constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-pseudo-checkbox"]],hostAttrs:[1,"mat-pseudo-checkbox"],hostVars:12,hostBindings:function(i,r){i&2&&j("mat-pseudo-checkbox-indeterminate",r.state==="indeterminate")("mat-pseudo-checkbox-checked",r.state==="checked")("mat-pseudo-checkbox-disabled",r.disabled)("mat-pseudo-checkbox-minimal",r.appearance==="minimal")("mat-pseudo-checkbox-full",r.appearance==="full")("_mat-animation-noopable",r._animationsDisabled)},inputs:{state:"state",disabled:"disabled",appearance:"appearance"},decls:0,vars:0,template:function(i,r){},styles:[`.mat-pseudo-checkbox {
  border-radius: 2px;
  cursor: pointer;
  display: inline-block;
  vertical-align: middle;
  box-sizing: border-box;
  position: relative;
  flex-shrink: 0;
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 0.1), background-color 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox::after {
  position: absolute;
  opacity: 0;
  content: "";
  border-bottom: 2px solid currentColor;
  transition: opacity 90ms cubic-bezier(0, 0, 0.2, 0.1);
}
.mat-pseudo-checkbox._mat-animation-noopable {
  transition: none !important;
  animation: none !important;
}
.mat-pseudo-checkbox._mat-animation-noopable::after {
  transition: none;
}

.mat-pseudo-checkbox-disabled {
  cursor: default;
}

.mat-pseudo-checkbox-indeterminate::after {
  left: 1px;
  opacity: 1;
  border-radius: 2px;
}

.mat-pseudo-checkbox-checked::after {
  left: 1px;
  border-left: 2px solid currentColor;
  transform: rotate(-45deg);
  opacity: 1;
  box-sizing: content-box;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-minimal-selected-checkmark-color, var(--mat-sys-primary));
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-pseudo-checkbox-full {
  border-color: var(--mat-pseudo-checkbox-full-unselected-icon-color, var(--mat-sys-on-surface-variant));
  border-width: 2px;
  border-style: solid;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-disabled {
  border-color: var(--mat-pseudo-checkbox-full-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate {
  background-color: var(--mat-pseudo-checkbox-full-selected-icon-color, var(--mat-sys-primary));
  border-color: transparent;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  color: var(--mat-pseudo-checkbox-full-selected-checkmark-color, var(--mat-sys-on-primary));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled {
  background-color: var(--mat-pseudo-checkbox-full-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked.mat-pseudo-checkbox-disabled::after, .mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate.mat-pseudo-checkbox-disabled::after {
  color: var(--mat-pseudo-checkbox-full-disabled-selected-checkmark-color, var(--mat-sys-surface));
}

.mat-pseudo-checkbox {
  width: 18px;
  height: 18px;
}

.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-checked::after {
  width: 14px;
  height: 6px;
  transform-origin: center;
  top: -4.2426406871px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-minimal.mat-pseudo-checkbox-indeterminate::after {
  top: 8px;
  width: 16px;
}

.mat-pseudo-checkbox-full.mat-pseudo-checkbox-checked::after {
  width: 10px;
  height: 4px;
  transform-origin: center;
  top: -2.8284271247px;
  left: 0;
  bottom: 0;
  right: 0;
  margin: auto;
}
.mat-pseudo-checkbox-full.mat-pseudo-checkbox-indeterminate::after {
  top: 6px;
  width: 12px;
}
`],encapsulation:2,changeDetection:0})}return n})();var tF=["text"],nF=[[["mat-icon"]],"*"],iF=["mat-icon","*"];function rF(n,t){if(n&1&&Z(0,"mat-pseudo-checkbox",1),n&2){let e=H();R("disabled",e.disabled)("state",e.selected?"checked":"unchecked")}}function oF(n,t){if(n&1&&Z(0,"mat-pseudo-checkbox",3),n&2){let e=H();R("disabled",e.disabled)}}function sF(n,t){if(n&1&&(_(0,"span",4),E(1),g()),n&2){let e=H();y(),Ye("(",e.group.label,")")}}var N_=new v("MAT_OPTION_PARENT_COMPONENT"),F_=new v("MatOptgroup");var O_=class{source;isUserInput;constructor(t,e=!1){this.source=t,this.isUserInput=e}},gs=(()=>{class n{_element=u(P);_changeDetectorRef=u(Ae);_parent=u(N_,{optional:!0});group=u(F_,{optional:!0});_signalDisableRipple=!1;_selected=!1;_active=!1;_mostRecentViewValue="";get multiple(){return this._parent&&this._parent.multiple}get selected(){return this._selected}value;id=u(Ie).getId("mat-option-");get disabled(){return this.group&&this.group.disabled||this._disabled()}set disabled(e){this._disabled.set(e)}_disabled=Oe(!1);get disableRipple(){return this._signalDisableRipple?this._parent.disableRipple():!!this._parent?.disableRipple}get hideSingleSelectionIndicator(){return!!(this._parent&&this._parent.hideSingleSelectionIndicator)}onSelectionChange=new z;_text;_stateChanges=new k;constructor(){let e=u(it);e.load(qi),e.load(is),this._signalDisableRipple=!!this._parent&&Hn(this._parent.disableRipple)}get active(){return this._active}get viewValue(){return(this._text?.nativeElement.textContent||"").trim()}select(e=!0){this._selected||(this._selected=!0,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}deselect(e=!0){this._selected&&(this._selected=!1,this._changeDetectorRef.markForCheck(),e&&this._emitSelectionChangeEvent())}focus(e,i){let r=this._getHostElement();typeof r.focus=="function"&&r.focus(i)}setActiveStyles(){this._active||(this._active=!0,this._changeDetectorRef.markForCheck())}setInactiveStyles(){this._active&&(this._active=!1,this._changeDetectorRef.markForCheck())}getLabel(){return this.viewValue}_handleKeydown(e){(e.keyCode===13||e.keyCode===32)&&!ut(e)&&(this._selectViaInteraction(),e.preventDefault())}_selectViaInteraction(){this.disabled||(this._selected=this.multiple?!this._selected:!0,this._changeDetectorRef.markForCheck(),this._emitSelectionChangeEvent(!0))}_getTabIndex(){return this.disabled?"-1":"0"}_getHostElement(){return this._element.nativeElement}ngAfterViewChecked(){if(this._selected){let e=this.viewValue;e!==this._mostRecentViewValue&&(this._mostRecentViewValue&&this._stateChanges.next(),this._mostRecentViewValue=e)}}ngOnDestroy(){this._stateChanges.complete()}_emitSelectionChangeEvent(e=!1){this.onSelectionChange.emit(new O_(this,e))}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-option"]],viewQuery:function(i,r){if(i&1&&Te(tF,7),i&2){let o;G(o=W())&&(r._text=o.first)}},hostAttrs:["role","option",1,"mat-mdc-option","mdc-list-item"],hostVars:11,hostBindings:function(i,r){i&1&&V("click",function(){return r._selectViaInteraction()})("keydown",function(s){return r._handleKeydown(s)}),i&2&&(Nt("id",r.id),J("aria-selected",r.selected)("aria-disabled",r.disabled.toString()),j("mdc-list-item--selected",r.selected)("mat-mdc-option-multiple",r.multiple)("mat-mdc-option-active",r.active)("mdc-list-item--disabled",r.disabled))},inputs:{value:"value",id:"id",disabled:[2,"disabled","disabled",N]},outputs:{onSelectionChange:"onSelectionChange"},exportAs:["matOption"],ngContentSelectors:iF,decls:8,vars:5,consts:[["text",""],["aria-hidden","true",1,"mat-mdc-option-pseudo-checkbox",3,"disabled","state"],[1,"mdc-list-item__primary-text"],["state","checked","aria-hidden","true","appearance","minimal",1,"mat-mdc-option-pseudo-checkbox",3,"disabled"],[1,"cdk-visually-hidden"],["aria-hidden","true","mat-ripple","",1,"mat-mdc-option-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled"]],template:function(i,r){i&1&&(Ee(nF),ce(0,rF,1,2,"mat-pseudo-checkbox",1),ie(1),_(2,"span",2,0),ie(4,1),g(),ce(5,oF,1,1,"mat-pseudo-checkbox",3),ce(6,sF,2,1,"span",4),Z(7,"div",5)),i&2&&(de(r.multiple?0:-1),y(5),de(!r.multiple&&r.selected&&!r.hideSingleSelectionIndicator?5:-1),y(),de(r.group&&r.group._inert?6:-1),y(),R("matRippleTrigger",r._getHostElement())("matRippleDisabled",r.disabled||r.disableRipple))},dependencies:[sm,ls],styles:[`.mat-mdc-option {
  -webkit-user-select: none;
  user-select: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: flex;
  position: relative;
  align-items: center;
  justify-content: flex-start;
  overflow: hidden;
  min-height: 48px;
  padding: 0 16px;
  cursor: pointer;
  -webkit-tap-highlight-color: transparent;
  color: var(--mat-option-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-option-label-text-font, var(--mat-sys-label-large-font));
  line-height: var(--mat-option-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-size: var(--mat-option-label-text-size, var(--mat-sys-body-large-size));
  letter-spacing: var(--mat-option-label-text-tracking, var(--mat-sys-label-large-tracking));
  font-weight: var(--mat-option-label-text-weight, var(--mat-sys-body-large-weight));
}
.mat-mdc-option:hover:not(.mdc-list-item--disabled) {
  background-color: var(--mat-option-hover-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-hover-state-layer-opacity) * 100%), transparent));
}
.mat-mdc-option:focus.mdc-list-item, .mat-mdc-option.mat-mdc-option-active.mdc-list-item {
  background-color: var(--mat-option-focus-state-layer-color, color-mix(in srgb, var(--mat-sys-on-surface) calc(var(--mat-sys-focus-state-layer-opacity) * 100%), transparent));
  outline: 0;
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) {
  background-color: var(--mat-option-selected-state-layer-color, var(--mat-sys-secondary-container));
}
.mat-mdc-option.mdc-list-item--selected:not(.mdc-list-item--disabled):not(.mat-mdc-option-active, .mat-mdc-option-multiple, :focus, :hover) .mdc-list-item__primary-text {
  color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-option-selected-state-label-text-color, var(--mat-sys-on-secondary-container));
}
.mat-mdc-option.mdc-list-item {
  align-items: center;
  background: transparent;
}
.mat-mdc-option.mdc-list-item--disabled {
  cursor: default;
  pointer-events: none;
}
.mat-mdc-option.mdc-list-item--disabled .mat-mdc-option-pseudo-checkbox, .mat-mdc-option.mdc-list-item--disabled .mdc-list-item__primary-text, .mat-mdc-option.mdc-list-item--disabled > mat-icon {
  opacity: 0.38;
}
.mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 32px;
}
[dir=rtl] .mat-mdc-optgroup .mat-mdc-option:not(.mat-mdc-option-multiple) {
  padding-left: 16px;
  padding-right: 32px;
}
.mat-mdc-option .mat-icon,
.mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-icon,
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-full {
  margin-right: 0;
  margin-left: 16px;
}
.mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-left: 16px;
  flex-shrink: 0;
}
[dir=rtl] .mat-mdc-option .mat-pseudo-checkbox-minimal {
  margin-right: 16px;
  margin-left: 0;
}
.mat-mdc-option .mat-mdc-option-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}
.mat-mdc-option .mdc-list-item__primary-text {
  white-space: normal;
  font-size: inherit;
  font-weight: inherit;
  letter-spacing: inherit;
  line-height: inherit;
  font-family: inherit;
  text-decoration: inherit;
  text-transform: inherit;
  margin-right: auto;
}
[dir=rtl] .mat-mdc-option .mdc-list-item__primary-text {
  margin-right: 0;
  margin-left: auto;
}
@media (forced-colors: active) {
  .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    content: "";
    position: absolute;
    top: 50%;
    right: 16px;
    transform: translateY(-50%);
    width: 10px;
    height: 0;
    border-bottom: solid 10px;
    border-radius: 10px;
  }
  [dir=rtl] .mat-mdc-option.mdc-list-item--selected:not(:has(.mat-mdc-option-pseudo-checkbox))::after {
    right: auto;
    left: 16px;
  }
}

.mat-mdc-option-multiple {
  --mat-list-list-item-selected-container-color: var(--mat-list-list-item-container-color, transparent);
}

.mat-mdc-option-active .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return n})();function IE(n,t,e){if(e.length){let i=t.toArray(),r=e.toArray(),o=0;for(let s=0;s<n+1;s++)i[s].group&&i[s].group===r[o]&&o++;return o}return 0}function ME(n,t,e,i){return n<e?n:n+t>e+i?Math.max(0,n-i+t):e}var am=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe]})}return n})();var P_=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[Qi,am,gs,oe]})}return n})();var aF=["trigger"],lF=["panel"],cF=[[["mat-select-trigger"]],"*"],dF=["mat-select-trigger","*"];function uF(n,t){if(n&1&&(_(0,"span",4),E(1),g()),n&2){let e=H();y(),It(e.placeholder)}}function mF(n,t){n&1&&ie(0)}function hF(n,t){if(n&1&&(_(0,"span",11),E(1),g()),n&2){let e=H(2);y(),It(e.triggerValue)}}function fF(n,t){if(n&1&&(_(0,"span",5),ce(1,mF,1,0)(2,hF,2,1,"span",11),g()),n&2){let e=H();y(),de(e.customTrigger?1:2)}}function pF(n,t){if(n&1){let e=Je();_(0,"div",12,1),V("keydown",function(r){He(e);let o=H();return Ue(o._handleKeydown(r))}),ie(2,1),g()}if(n&2){let e=H();St(e.panelClass),j("mat-select-panel-animations-enabled",!e._animationsDisabled)("mat-primary",(e._parentFormField==null?null:e._parentFormField.color)==="primary")("mat-accent",(e._parentFormField==null?null:e._parentFormField.color)==="accent")("mat-warn",(e._parentFormField==null?null:e._parentFormField.color)==="warn")("mat-undefined",!(e._parentFormField!=null&&e._parentFormField.color)),J("id",e.id+"-panel")("aria-multiselectable",e.multiple)("aria-label",e.ariaLabel||null)("aria-labelledby",e._getPanelAriaLabelledby())}}var gF=new v("mat-select-scroll-strategy",{providedIn:"root",factory:()=>{let n=u($);return()=>Jr(n)}}),_F=new v("MAT_SELECT_CONFIG"),vF=new v("MatSelectTrigger"),L_=class{source;value;constructor(t,e){this.source=t,this.value=e}},TE=(()=>{class n{_viewportRuler=u(Sn);_changeDetectorRef=u(Ae);_elementRef=u(P);_dir=u(st,{optional:!0});_idGenerator=u(Ie);_renderer=u(Ke);_parentFormField=u(ol,{optional:!0});ngControl=u(En,{self:!0,optional:!0});_liveAnnouncer=u(Xa);_defaultOptions=u(_F,{optional:!0});_animationsDisabled=Fe();_popoverLocation;_initialized=new k;_cleanupDetach;options;optionGroups;customTrigger;_positions=[{originX:"start",originY:"bottom",overlayX:"start",overlayY:"top"},{originX:"end",originY:"bottom",overlayX:"end",overlayY:"top"},{originX:"start",originY:"top",overlayX:"start",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"},{originX:"end",originY:"top",overlayX:"end",overlayY:"bottom",panelClass:"mat-mdc-select-panel-above"}];_scrollOptionIntoView(e){let i=this.options.toArray()[e];if(i){let r=this.panel.nativeElement,o=IE(e,this.options,this.optionGroups),s=i._getHostElement();e===0&&o===1?r.scrollTop=0:r.scrollTop=ME(s.offsetTop,s.offsetHeight,r.scrollTop,r.offsetHeight)}}_positioningSettled(){this._scrollOptionIntoView(this._keyManager.activeItemIndex||0)}_getChangeEvent(e){return new L_(this,e)}_scrollStrategyFactory=u(gF);_panelOpen=!1;_compareWith=(e,i)=>e===i;_uid=this._idGenerator.getId("mat-select-");_triggerAriaLabelledBy=null;_previousControl;_destroy=new k;_errorStateTracker;stateChanges=new k;disableAutomaticLabeling=!0;userAriaDescribedBy;_selectionModel;_keyManager;_preferredOverlayOrigin;_overlayWidth;_onChange=()=>{};_onTouched=()=>{};_valueId=this._idGenerator.getId("mat-select-value-");_scrollStrategy;_overlayPanelClass=this._defaultOptions?.overlayPanelClass||"";get focused(){return this._focused||this._panelOpen}_focused=!1;controlType="mat-select";trigger;panel;_overlayDir;panelClass;disabled=!1;get disableRipple(){return this._disableRipple()}set disableRipple(e){this._disableRipple.set(e)}_disableRipple=Oe(!1);tabIndex=0;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._syncParentProperties()}_hideSingleSelectionIndicator=this._defaultOptions?.hideSingleSelectionIndicator??!1;get placeholder(){return this._placeholder}set placeholder(e){this._placeholder=e,this.stateChanges.next()}_placeholder;get required(){return this._required??this.ngControl?.control?.hasValidator(Zo.required)??!1}set required(e){this._required=e,this.stateChanges.next()}_required;get multiple(){return this._multiple}set multiple(e){this._selectionModel,this._multiple=e}_multiple=!1;disableOptionCentering=this._defaultOptions?.disableOptionCentering??!1;get compareWith(){return this._compareWith}set compareWith(e){this._compareWith=e,this._selectionModel&&this._initializeSelection()}get value(){return this._value}set value(e){this._assignValue(e)&&this._onChange(e)}_value;ariaLabel="";ariaLabelledby;get errorStateMatcher(){return this._errorStateTracker.matcher}set errorStateMatcher(e){this._errorStateTracker.matcher=e}typeaheadDebounceInterval;sortComparator;get id(){return this._id}set id(e){this._id=e||this._uid,this.stateChanges.next()}_id;get errorState(){return this._errorStateTracker.errorState}set errorState(e){this._errorStateTracker.errorState=e}panelWidth=this._defaultOptions&&typeof this._defaultOptions.panelWidth<"u"?this._defaultOptions.panelWidth:"auto";canSelectNullableOptions=this._defaultOptions?.canSelectNullableOptions??!1;optionSelectionChanges=hr(()=>{let e=this.options;return e?e.changes.pipe(jt(e),fr(()=>Zt(...e.map(i=>i.onSelectionChange)))):this._initialized.pipe(fr(()=>this.optionSelectionChanges))});openedChange=new z;_openedStream=this.openedChange.pipe(ze(e=>e),ye(()=>{}));_closedStream=this.openedChange.pipe(ze(e=>!e),ye(()=>{}));selectionChange=new z;valueChange=new z;constructor(){let e=u(cs),i=u(Ga,{optional:!0}),r=u($r,{optional:!0}),o=u(new an("tabindex"),{optional:!0}),s=u(hl,{optional:!0});this.ngControl&&(this.ngControl.valueAccessor=this),this._defaultOptions?.typeaheadDebounceInterval!=null&&(this.typeaheadDebounceInterval=this._defaultOptions.typeaheadDebounceInterval),this._errorStateTracker=new ds(e,this.ngControl,r,i,this.stateChanges),this._scrollStrategy=this._scrollStrategyFactory(),this.tabIndex=o==null?0:parseInt(o)||0,this._popoverLocation=s?.usePopover===!1?null:"inline",this.id=this.id}ngOnInit(){this._selectionModel=new Kr(this.multiple),this.stateChanges.next(),this._viewportRuler.change().pipe(Ce(this._destroy)).subscribe(()=>{this.panelOpen&&(this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._changeDetectorRef.detectChanges())})}ngAfterContentInit(){this._initialized.next(),this._initialized.complete(),this._initKeyManager(),this._selectionModel.changed.pipe(Ce(this._destroy)).subscribe(e=>{e.added.forEach(i=>i.select()),e.removed.forEach(i=>i.deselect())}),this.options.changes.pipe(jt(null),Ce(this._destroy)).subscribe(()=>{this._resetOptions(),this._initializeSelection()})}ngDoCheck(){let e=this._getTriggerAriaLabelledby(),i=this.ngControl;if(e!==this._triggerAriaLabelledBy){let r=this._elementRef.nativeElement;this._triggerAriaLabelledBy=e,e?r.setAttribute("aria-labelledby",e):r.removeAttribute("aria-labelledby")}i&&(this._previousControl!==i.control&&(this._previousControl!==void 0&&i.disabled!==null&&i.disabled!==this.disabled&&(this.disabled=i.disabled),this._previousControl=i.control),this.updateErrorState())}ngOnChanges(e){(e.disabled||e.userAriaDescribedBy)&&this.stateChanges.next(),e.typeaheadDebounceInterval&&this._keyManager&&this._keyManager.withTypeAhead(this.typeaheadDebounceInterval),e.panelClass&&this.panelClass instanceof Set&&(this.panelClass=Array.from(this.panelClass))}ngOnDestroy(){this._cleanupDetach?.(),this._keyManager?.destroy(),this._destroy.next(),this._destroy.complete(),this.stateChanges.complete(),this._clearFromModal()}toggle(){this.panelOpen?this.close():this.open()}open(){this._canOpen()&&(this._parentFormField&&(this._preferredOverlayOrigin=this._parentFormField.getConnectedOverlayOrigin()),this._cleanupDetach?.(),this._overlayWidth=this._getOverlayWidth(this._preferredOverlayOrigin),this._applyModalPanelOwnership(),this._panelOpen=!0,this._overlayDir.positionChange.pipe(Vt(1)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this._positioningSettled()}),this._overlayDir.attachOverlay(),this._keyManager.withHorizontalOrientation(null),this._highlightCorrectOption(),this._changeDetectorRef.markForCheck(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!0)))}_trackedModal=null;_applyModalPanelOwnership(){let e=this._elementRef.nativeElement.closest('body > .cdk-overlay-container [aria-modal="true"]');if(!e)return;let i=`${this.id}-panel`;this._trackedModal&&Gu(this._trackedModal,"aria-owns",i),m_(e,"aria-owns",i),this._trackedModal=e}_clearFromModal(){if(!this._trackedModal)return;let e=`${this.id}-panel`;Gu(this._trackedModal,"aria-owns",e),this._trackedModal=null}close(){this._panelOpen&&(this._panelOpen=!1,this._exitAndDetach(),this._keyManager.withHorizontalOrientation(this._isRtl()?"rtl":"ltr"),this._changeDetectorRef.markForCheck(),this._onTouched(),this.stateChanges.next(),Promise.resolve().then(()=>this.openedChange.emit(!1)))}_exitAndDetach(){if(this._animationsDisabled||!this.panel){this._detachOverlay();return}this._cleanupDetach?.(),this._cleanupDetach=()=>{i(),clearTimeout(r),this._cleanupDetach=void 0};let e=this.panel.nativeElement,i=this._renderer.listen(e,"animationend",o=>{o.animationName==="_mat-select-exit"&&(this._cleanupDetach?.(),this._detachOverlay())}),r=setTimeout(()=>{this._cleanupDetach?.(),this._detachOverlay()},200);e.classList.add("mat-select-panel-exit")}_detachOverlay(){this._overlayDir.detachOverlay(),this._changeDetectorRef.markForCheck()}writeValue(e){this._assignValue(e)}registerOnChange(e){this._onChange=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e,this._changeDetectorRef.markForCheck(),this.stateChanges.next()}get panelOpen(){return this._panelOpen}get selected(){return this.multiple?this._selectionModel?.selected||[]:this._selectionModel?.selected[0]}get triggerValue(){if(this.empty)return"";if(this._multiple){let e=this._selectionModel.selected.map(i=>i.viewValue);return this._isRtl()&&e.reverse(),e.join(", ")}return this._selectionModel.selected[0].viewValue}updateErrorState(){this._errorStateTracker.updateErrorState()}_isRtl(){return this._dir?this._dir.value==="rtl":!1}_handleKeydown(e){this.disabled||(this.panelOpen?this._handleOpenKeydown(e):this._handleClosedKeydown(e))}_handleClosedKeydown(e){let i=e.keyCode,r=i===40||i===38||i===37||i===39,o=i===13||i===32,s=this._keyManager;if(!s.isTyping()&&o&&!ut(e)||(this.multiple||e.altKey)&&r)e.preventDefault(),this.open();else if(!this.multiple){let a=this.selected;s.onKeydown(e);let l=this.selected;l&&a!==l&&this._liveAnnouncer.announce(l.viewValue,1e4)}}_handleOpenKeydown(e){let i=this._keyManager,r=e.keyCode,o=r===40||r===38,s=i.isTyping();if(o&&e.altKey)e.preventDefault(),this.close();else if(!s&&(r===13||r===32)&&i.activeItem&&!ut(e))e.preventDefault(),i.activeItem._selectViaInteraction();else if(!s&&this._multiple&&r===65&&e.ctrlKey){e.preventDefault();let a=this.options.some(l=>!l.disabled&&!l.selected);this.options.forEach(l=>{l.disabled||(a?l.select():l.deselect())})}else{let a=i.activeItemIndex;i.onKeydown(e),this._multiple&&o&&e.shiftKey&&i.activeItem&&i.activeItemIndex!==a&&i.activeItem._selectViaInteraction()}}_handleOverlayKeydown(e){e.keyCode===27&&!ut(e)&&(e.preventDefault(),this.close())}_onFocus(){this.disabled||(this._focused=!0,this.stateChanges.next())}_onBlur(){this._focused=!1,this._keyManager?.cancelTypeahead(),!this.disabled&&!this.panelOpen&&(this._onTouched(),this._changeDetectorRef.markForCheck(),this.stateChanges.next())}get empty(){return!this._selectionModel||this._selectionModel.isEmpty()}_initializeSelection(){Promise.resolve().then(()=>{this.ngControl&&(this._value=this.ngControl.value),this._setSelectionByValue(this._value),this.stateChanges.next()})}_setSelectionByValue(e){if(this.options.forEach(i=>i.setInactiveStyles()),this._selectionModel.clear(),this.multiple&&e)Array.isArray(e),e.forEach(i=>this._selectOptionByValue(i)),this._sortValues();else{let i=this._selectOptionByValue(e);i?this._keyManager.updateActiveItem(i):this.panelOpen||this._keyManager.updateActiveItem(-1)}this._changeDetectorRef.markForCheck()}_selectOptionByValue(e){let i=this.options.find(r=>{if(this._selectionModel.isSelected(r))return!1;try{return(r.value!=null||this.canSelectNullableOptions)&&this._compareWith(r.value,e)}catch(o){return!1}});return i&&this._selectionModel.select(i),i}_assignValue(e){return e!==this._value||this._multiple&&Array.isArray(e)?(this.options&&this._setSelectionByValue(e),this._value=e,!0):!1}_skipPredicate=e=>this.panelOpen?!1:e.disabled;_getOverlayWidth(e){return this.panelWidth==="auto"?(e instanceof hs?e.elementRef:e||this._elementRef).nativeElement.getBoundingClientRect().width:this.panelWidth===null?"":this.panelWidth}_syncParentProperties(){if(this.options)for(let e of this.options)e._changeDetectorRef.markForCheck()}_initKeyManager(){this._keyManager=new tl(this.options).withTypeAhead(this.typeaheadDebounceInterval).withVerticalOrientation().withHorizontalOrientation(this._isRtl()?"rtl":"ltr").withHomeAndEnd().withPageUpDown().withAllowedModifierKeys(["shiftKey"]).skipPredicate(this._skipPredicate),this._keyManager.tabOut.subscribe(()=>{this.panelOpen&&(!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction(),this.focus(),this.close())}),this._keyManager.change.subscribe(()=>{this._panelOpen&&this.panel?this._scrollOptionIntoView(this._keyManager.activeItemIndex||0):!this._panelOpen&&!this.multiple&&this._keyManager.activeItem&&this._keyManager.activeItem._selectViaInteraction()})}_resetOptions(){let e=Zt(this.options.changes,this._destroy);this.optionSelectionChanges.pipe(Ce(e)).subscribe(i=>{this._onSelect(i.source,i.isUserInput),i.isUserInput&&!this.multiple&&this._panelOpen&&(this.close(),this.focus())}),Zt(...this.options.map(i=>i._stateChanges)).pipe(Ce(e)).subscribe(()=>{this._changeDetectorRef.detectChanges(),this.stateChanges.next()})}_onSelect(e,i){let r=this._selectionModel.isSelected(e);!this.canSelectNullableOptions&&e.value==null&&!this._multiple?(e.deselect(),this._selectionModel.clear(),this.value!=null&&this._propagateChanges(e.value)):(r!==e.selected&&(e.selected?this._selectionModel.select(e):this._selectionModel.deselect(e)),i&&this._keyManager.setActiveItem(e),this.multiple&&(this._sortValues(),i&&this.focus())),r!==this._selectionModel.isSelected(e)&&this._propagateChanges(),this.stateChanges.next()}_sortValues(){if(this.multiple){let e=this.options.toArray();this._selectionModel.sort((i,r)=>this.sortComparator?this.sortComparator(i,r,e):e.indexOf(i)-e.indexOf(r)),this.stateChanges.next()}}_propagateChanges(e){let i;this.multiple?i=this.selected.map(r=>r.value):i=this.selected?this.selected.value:e,this._value=i,this.valueChange.emit(i),this._onChange(i),this.selectionChange.emit(this._getChangeEvent(i)),this._changeDetectorRef.markForCheck()}_highlightCorrectOption(){if(this._keyManager)if(this.empty){let e=-1;for(let i=0;i<this.options.length;i++)if(!this.options.get(i).disabled){e=i;break}this._keyManager.setActiveItem(e)}else this._keyManager.setActiveItem(this._selectionModel.selected[0])}_canOpen(){return!this._panelOpen&&!this.disabled&&this.options?.length>0&&!!this._overlayDir}focus(e){this._elementRef.nativeElement.focus(e)}_getPanelAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||null,i=e?e+" ":"";return this.ariaLabelledby?i+this.ariaLabelledby:e}_getAriaActiveDescendant(){return this.panelOpen&&this._keyManager&&this._keyManager.activeItem?this._keyManager.activeItem.id:null}_getTriggerAriaLabelledby(){if(this.ariaLabel)return null;let e=this._parentFormField?.getLabelId()||"";return this.ariaLabelledby&&(e+=" "+this.ariaLabelledby),e||(e=this._valueId),e}get describedByIds(){return this._elementRef.nativeElement.getAttribute("aria-describedby")?.split(" ")||[]}setDescribedByIds(e){let i=this._elementRef.nativeElement;e.length?i.setAttribute("aria-describedby",e.join(" ")):i.removeAttribute("aria-describedby")}onContainerClick(e){let i=ft(e);i&&(i.tagName==="MAT-OPTION"||i.classList.contains("cdk-overlay-backdrop")||i.closest(".mat-mdc-select-panel"))||(this.focus(),this.open())}get shouldLabelFloat(){return this.panelOpen||!this.empty||this.focused&&!!this.placeholder}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-select"]],contentQueries:function(i,r,o){if(i&1&&kt(o,vF,5)(o,gs,5)(o,F_,5),i&2){let s;G(s=W())&&(r.customTrigger=s.first),G(s=W())&&(r.options=s),G(s=W())&&(r.optionGroups=s)}},viewQuery:function(i,r){if(i&1&&Te(aF,5)(lF,5)(om,5),i&2){let o;G(o=W())&&(r.trigger=o.first),G(o=W())&&(r.panel=o.first),G(o=W())&&(r._overlayDir=o.first)}},hostAttrs:["role","combobox","aria-haspopup","listbox",1,"mat-mdc-select"],hostVars:21,hostBindings:function(i,r){i&1&&V("keydown",function(s){return r._handleKeydown(s)})("focus",function(){return r._onFocus()})("blur",function(){return r._onBlur()}),i&2&&(J("id",r.id)("tabindex",r.disabled?-1:r.tabIndex)("aria-controls",r.panelOpen?r.id+"-panel":null)("aria-expanded",r.panelOpen)("aria-label",r.ariaLabel||null)("aria-required",r.required.toString())("aria-disabled",r.disabled.toString())("aria-invalid",r.errorState)("aria-activedescendant",r._getAriaActiveDescendant()),j("mat-mdc-select-disabled",r.disabled)("mat-mdc-select-invalid",r.errorState)("mat-mdc-select-required",r.required)("mat-mdc-select-empty",r.empty)("mat-mdc-select-multiple",r.multiple)("mat-select-open",r.panelOpen))},inputs:{userAriaDescribedBy:[0,"aria-describedby","userAriaDescribedBy"],panelClass:"panelClass",disabled:[2,"disabled","disabled",N],disableRipple:[2,"disableRipple","disableRipple",N],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?0:ht(e)],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",N],placeholder:"placeholder",required:[2,"required","required",N],multiple:[2,"multiple","multiple",N],disableOptionCentering:[2,"disableOptionCentering","disableOptionCentering",N],compareWith:"compareWith",value:"value",ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],errorStateMatcher:"errorStateMatcher",typeaheadDebounceInterval:[2,"typeaheadDebounceInterval","typeaheadDebounceInterval",ht],sortComparator:"sortComparator",id:"id",panelWidth:"panelWidth",canSelectNullableOptions:[2,"canSelectNullableOptions","canSelectNullableOptions",N]},outputs:{openedChange:"openedChange",_openedStream:"opened",_closedStream:"closed",selectionChange:"selectionChange",valueChange:"valueChange"},exportAs:["matSelect"],features:[ve([{provide:rl,useExisting:n},{provide:N_,useExisting:n}]),Le],ngContentSelectors:dF,decls:11,vars:10,consts:[["fallbackOverlayOrigin","cdkOverlayOrigin","trigger",""],["panel",""],["cdk-overlay-origin","",1,"mat-mdc-select-trigger",3,"click"],[1,"mat-mdc-select-value"],[1,"mat-mdc-select-placeholder","mat-mdc-select-min-line"],[1,"mat-mdc-select-value-text"],[1,"mat-mdc-select-arrow-wrapper"],[1,"mat-mdc-select-arrow"],["viewBox","0 0 24 24","width","24px","height","24px","focusable","false","aria-hidden","true"],["d","M7 10l5 5 5-5z"],["cdk-connected-overlay","","cdkConnectedOverlayHasBackdrop","","cdkConnectedOverlayBackdropClass","cdk-overlay-transparent-backdrop",3,"detach","backdropClick","overlayKeydown","cdkConnectedOverlayDisableClose","cdkConnectedOverlayPanelClass","cdkConnectedOverlayScrollStrategy","cdkConnectedOverlayOrigin","cdkConnectedOverlayPositions","cdkConnectedOverlayWidth","cdkConnectedOverlayFlexibleDimensions","cdkConnectedOverlayUsePopover"],[1,"mat-mdc-select-min-line"],["role","listbox","tabindex","-1",1,"mat-mdc-select-panel","mdc-menu-surface","mdc-menu-surface--open",3,"keydown"]],template:function(i,r){if(i&1&&(Ee(cF),_(0,"div",2,0),V("click",function(){return r.open()}),_(3,"div",3),ce(4,uF,2,1,"span",4)(5,fF,3,1,"span",5),g(),_(6,"div",6)(7,"div",7),yt(),_(8,"svg",8),Z(9,"path",9),g()()()(),fe(10,pF,3,16,"ng-template",10),V("detach",function(){return r.close()})("backdropClick",function(){return r.close()})("overlayKeydown",function(s){return r._handleOverlayKeydown(s)})),i&2){let o=dt(1);y(3),J("id",r._valueId),y(),de(r.empty?4:5),y(6),R("cdkConnectedOverlayDisableClose",!0)("cdkConnectedOverlayPanelClass",r._overlayPanelClass)("cdkConnectedOverlayScrollStrategy",r._scrollStrategy)("cdkConnectedOverlayOrigin",r._preferredOverlayOrigin||o)("cdkConnectedOverlayPositions",r._positions)("cdkConnectedOverlayWidth",r._overlayWidth)("cdkConnectedOverlayFlexibleDimensions",!0)("cdkConnectedOverlayUsePopover",r._popoverLocation)}},dependencies:[hs,om],styles:[`@keyframes _mat-select-enter {
  from {
    opacity: 0;
    transform: scaleY(0.8);
  }
  to {
    opacity: 1;
    transform: none;
  }
}
@keyframes _mat-select-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-select {
  display: inline-block;
  width: 100%;
  outline: none;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-select-enabled-trigger-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-select-trigger-text-font, var(--mat-sys-body-large-font));
  line-height: var(--mat-select-trigger-text-line-height, var(--mat-sys-body-large-line-height));
  font-size: var(--mat-select-trigger-text-size, var(--mat-sys-body-large-size));
  font-weight: var(--mat-select-trigger-text-weight, var(--mat-sys-body-large-weight));
  letter-spacing: var(--mat-select-trigger-text-tracking, var(--mat-sys-body-large-tracking));
}

div.mat-mdc-select-panel {
  box-shadow: var(--mat-select-container-elevation-shadow, 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12));
}

.mat-mdc-select-disabled {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-mdc-select-disabled .mat-mdc-select-placeholder {
  color: var(--mat-select-disabled-trigger-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}

.mat-mdc-select-trigger {
  display: inline-flex;
  align-items: center;
  cursor: pointer;
  position: relative;
  box-sizing: border-box;
  width: 100%;
}
.mat-mdc-select-disabled .mat-mdc-select-trigger {
  -webkit-user-select: none;
  user-select: none;
  cursor: default;
}

.mat-mdc-select-value {
  width: 100%;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.mat-mdc-select-value-text {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.mat-mdc-select-arrow-wrapper {
  height: 24px;
  flex-shrink: 0;
  display: inline-flex;
  align-items: center;
}
.mat-form-field-appearance-fill .mdc-text-field--no-label .mat-mdc-select-arrow-wrapper {
  transform: none;
}

.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-invalid .mat-mdc-select-arrow,
.mat-form-field-invalid:not(.mat-form-field-disabled) .mat-mdc-form-field-infix::after {
  color: var(--mat-select-invalid-arrow-color, var(--mat-sys-error));
}

.mat-mdc-select-arrow {
  width: 10px;
  height: 5px;
  position: relative;
  color: var(--mat-select-enabled-arrow-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field.mat-focused .mat-mdc-select-arrow {
  color: var(--mat-select-focused-arrow-color, var(--mat-sys-primary));
}
.mat-mdc-form-field .mat-mdc-select.mat-mdc-select-disabled .mat-mdc-select-arrow {
  color: var(--mat-select-disabled-arrow-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-select-open .mat-mdc-select-arrow {
  transform: rotate(180deg);
}
.mat-form-field-animations-enabled .mat-mdc-select-arrow {
  transition: transform 80ms linear;
}
.mat-mdc-select-arrow svg {
  fill: currentColor;
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
}
@media (forced-colors: active) {
  .mat-mdc-select-arrow svg {
    fill: CanvasText;
  }
  .mat-mdc-select-disabled .mat-mdc-select-arrow svg {
    fill: GrayText;
  }
}

div.mat-mdc-select-panel {
  width: 100%;
  max-height: 275px;
  outline: 0;
  overflow: auto;
  padding: 8px 0;
  box-sizing: border-box;
  transform-origin: top center;
  border-radius: 0 0 4px 4px;
  position: relative;
  background-color: var(--mat-select-panel-background-color, var(--mat-sys-surface-container));
}
.mat-mdc-select-panel-above div.mat-mdc-select-panel {
  border-radius: 4px 4px 0 0;
  transform-origin: bottom center;
}
@media (forced-colors: active) {
  div.mat-mdc-select-panel {
    outline: solid 1px;
  }
}

.mat-select-panel-animations-enabled {
  animation: _mat-select-enter 120ms cubic-bezier(0, 0, 0.2, 1);
}
.mat-select-panel-animations-enabled.mat-select-panel-exit {
  animation: _mat-select-exit 100ms linear;
}

.mat-mdc-select-placeholder {
  transition: color 400ms 133.3333333333ms cubic-bezier(0.25, 0.8, 0.25, 1);
  color: var(--mat-select-placeholder-text-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-form-field:not(.mat-form-field-animations-enabled) .mat-mdc-select-placeholder, ._mat-animation-noopable .mat-mdc-select-placeholder {
  transition: none;
}
.mat-form-field-hide-placeholder .mat-mdc-select-placeholder {
  color: transparent;
  -webkit-text-fill-color: transparent;
  transition: none;
  display: block;
}

.mat-mdc-form-field-type-mat-select:not(.mat-form-field-disabled) .mat-mdc-text-field-wrapper {
  cursor: pointer;
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mat-mdc-floating-label {
  max-width: calc(100% - 18px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-fill .mdc-floating-label--float-above {
  max-width: calc(100% / 0.75 - 24px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-notched-outline__notch {
  max-width: calc(100% - 60px);
}
.mat-mdc-form-field-type-mat-select.mat-form-field-appearance-outline .mdc-text-field--label-floating .mdc-notched-outline__notch {
  max-width: calc(100% - 24px);
}

.mat-mdc-select-min-line:empty::before {
  content: " ";
  white-space: pre;
  width: 1px;
  display: inline-block;
  visibility: hidden;
}

.mat-form-field-appearance-fill .mat-mdc-select-arrow-wrapper {
  transform: var(--mat-select-arrow-transform, translateY(-8px));
}
`],encapsulation:2,changeDetection:0})}return n})();var AE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[Qn,P_,oe,qn,Yi,P_]})}return n})();var bF=["tooltip"],yF=20;var wF=new v("mat-tooltip-scroll-strategy",{providedIn:"root",factory:()=>{let n=u($);return()=>Jr(n,{scrollThrottle:yF})}}),CF=new v("mat-tooltip-default-options",{providedIn:"root",factory:()=>({showDelay:0,hideDelay:0,touchendHideDelay:1500})});var RE="tooltip-panel",DF={passive:!0},xF=8,EF=8,kF=24,SF=200,OE=(()=>{class n{_elementRef=u(P);_ngZone=u(F);_platform=u(_e);_ariaDescriber=u(h_);_focusMonitor=u(mi);_dir=u(st);_injector=u($);_viewContainerRef=u(tt);_mediaMatcher=u(os);_document=u(U);_renderer=u(Ke);_animationsDisabled=Fe();_defaultOptions=u(CF,{optional:!0});_overlayRef=null;_tooltipInstance=null;_overlayPanelClass;_portal;_position="below";_positionAtOrigin=!1;_disabled=!1;_tooltipClass;_viewInitialized=!1;_pointerExitEventsInitialized=!1;_tooltipComponent=IF;_viewportMargin=8;_currentPosition;_cssClassPrefix="mat-mdc";_ariaDescriptionPending=!1;_dirSubscribed=!1;get position(){return this._position}set position(e){e!==this._position&&(this._position=e,this._overlayRef&&(this._updatePosition(this._overlayRef),this._tooltipInstance?.show(0),this._overlayRef.updatePosition()))}get positionAtOrigin(){return this._positionAtOrigin}set positionAtOrigin(e){this._positionAtOrigin=fi(e),this._detach(),this._overlayRef=null}get disabled(){return this._disabled}set disabled(e){let i=fi(e);this._disabled!==i&&(this._disabled=i,i?this.hide(0):this._setupPointerEnterEventsIfNeeded(),this._syncAriaDescription(this.message))}get showDelay(){return this._showDelay}set showDelay(e){this._showDelay=ui(e)}_showDelay;get hideDelay(){return this._hideDelay}set hideDelay(e){this._hideDelay=ui(e),this._tooltipInstance&&(this._tooltipInstance._mouseLeaveHideDelay=this._hideDelay)}_hideDelay;touchGestures="auto";get message(){return this._message}set message(e){let i=this._message;this._message=e!=null?String(e).trim():"",!this._message&&this._isTooltipVisible()?this.hide(0):(this._setupPointerEnterEventsIfNeeded(),this._updateTooltipMessage()),this._syncAriaDescription(i)}_message="";get tooltipClass(){return this._tooltipClass}set tooltipClass(e){this._tooltipClass=e,this._tooltipInstance&&this._setTooltipClass(this._tooltipClass)}_eventCleanups=[];_touchstartTimeout=null;_destroyed=new k;_isDestroyed=!1;constructor(){let e=this._defaultOptions;e&&(this._showDelay=e.showDelay,this._hideDelay=e.hideDelay,e.position&&(this.position=e.position),e.positionAtOrigin&&(this.positionAtOrigin=e.positionAtOrigin),e.touchGestures&&(this.touchGestures=e.touchGestures),e.tooltipClass&&(this.tooltipClass=e.tooltipClass)),this._viewportMargin=xF}ngAfterViewInit(){this._viewInitialized=!0,this._setupPointerEnterEventsIfNeeded(),this._focusMonitor.monitor(this._elementRef).pipe(Ce(this._destroyed)).subscribe(e=>{e?e==="keyboard"&&this._ngZone.run(()=>this.show()):this._ngZone.run(()=>this.hide(0))})}ngOnDestroy(){let e=this._elementRef.nativeElement;this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this._overlayRef&&(this._overlayRef.dispose(),this._tooltipInstance=null),this._eventCleanups.forEach(i=>i()),this._eventCleanups.length=0,this._destroyed.next(),this._destroyed.complete(),this._isDestroyed=!0,this._ariaDescriber.removeDescription(e,this.message,"tooltip"),this._focusMonitor.stopMonitoring(e)}show(e=this.showDelay,i){if(this.disabled||!this.message||this._isTooltipVisible()){this._tooltipInstance?._cancelPendingAnimations();return}let r=this._createOverlay(i);this._detach(),this._portal=this._portal||new In(this._tooltipComponent,this._viewContainerRef);let o=this._tooltipInstance=r.attach(this._portal).instance;o._triggerElement=this._elementRef.nativeElement,o._mouseLeaveHideDelay=this._hideDelay,o.afterHidden().pipe(Ce(this._destroyed)).subscribe(()=>this._detach()),this._setTooltipClass(this._tooltipClass),this._updateTooltipMessage(),o.show(e)}hide(e=this.hideDelay){let i=this._tooltipInstance;i&&(i.isVisible()?i.hide(e):(i._cancelPendingAnimations(),this._detach()))}toggle(e){this._isTooltipVisible()?this.hide():this.show(void 0,e)}_isTooltipVisible(){return!!this._tooltipInstance&&this._tooltipInstance.isVisible()}_createOverlay(e){if(this._overlayRef){let s=this._overlayRef.getConfig().positionStrategy;if((!this.positionAtOrigin||!e)&&s._origin instanceof P)return this._overlayRef;this._detach()}let i=this._injector.get(Yr).getAncestorScrollContainers(this._elementRef),r=`${this._cssClassPrefix}-${RE}`,o=ml(this._injector,this.positionAtOrigin?e||this._elementRef:this._elementRef).withTransformOriginOn(`.${this._cssClassPrefix}-tooltip`).withFlexibleDimensions(!1).withViewportMargin(this._viewportMargin).withScrollableContainers(i).withPopoverLocation("global");return o.positionChanges.pipe(Ce(this._destroyed)).subscribe(s=>{this._updateCurrentPositionClass(s.connectionPair),this._tooltipInstance&&s.scrollableViewProperties.isOverlayClipped&&this._tooltipInstance.isVisible()&&this._ngZone.run(()=>this.hide(0))}),this._overlayRef=vi(this._injector,{direction:this._dir,positionStrategy:o,panelClass:this._overlayPanelClass?[...this._overlayPanelClass,r]:r,scrollStrategy:this._injector.get(wF)(),disableAnimations:this._animationsDisabled,eventPredicate:this._overlayEventPredicate}),this._updatePosition(this._overlayRef),this._overlayRef.detachments().pipe(Ce(this._destroyed)).subscribe(()=>this._detach()),this._overlayRef.outsidePointerEvents().pipe(Ce(this._destroyed)).subscribe(()=>this._tooltipInstance?._handleBodyInteraction()),this._overlayRef.keydownEvents().pipe(Ce(this._destroyed)).subscribe(s=>{s.preventDefault(),s.stopPropagation(),this._ngZone.run(()=>this.hide(0))}),this._defaultOptions?.disableTooltipInteractivity&&this._overlayRef.addPanelClass(`${this._cssClassPrefix}-tooltip-panel-non-interactive`),this._dirSubscribed||(this._dirSubscribed=!0,this._dir.change.pipe(Ce(this._destroyed)).subscribe(()=>{this._overlayRef&&this._updatePosition(this._overlayRef)})),this._overlayRef}_detach(){this._overlayRef&&this._overlayRef.hasAttached()&&this._overlayRef.detach(),this._tooltipInstance=null}_updatePosition(e){let i=e.getConfig().positionStrategy,r=this._getOrigin(),o=this._getOverlayPosition();i.withPositions([this._addOffset(M(M({},r.main),o.main)),this._addOffset(M(M({},r.fallback),o.fallback))])}_addOffset(e){let i=EF,r=!this._dir||this._dir.value=="ltr";return e.originY==="top"?e.offsetY=-i:e.originY==="bottom"?e.offsetY=i:e.originX==="start"?e.offsetX=r?-i:i:e.originX==="end"&&(e.offsetX=r?i:-i),e}_getOrigin(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"||i=="below"?r={originX:"center",originY:i=="above"?"top":"bottom"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={originX:"start",originY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={originX:"end",originY:"center"});let{x:o,y:s}=this._invertPosition(r.originX,r.originY);return{main:r,fallback:{originX:o,originY:s}}}_getOverlayPosition(){let e=!this._dir||this._dir.value=="ltr",i=this.position,r;i=="above"?r={overlayX:"center",overlayY:"bottom"}:i=="below"?r={overlayX:"center",overlayY:"top"}:i=="before"||i=="left"&&e||i=="right"&&!e?r={overlayX:"end",overlayY:"center"}:(i=="after"||i=="right"&&e||i=="left"&&!e)&&(r={overlayX:"start",overlayY:"center"});let{x:o,y:s}=this._invertPosition(r.overlayX,r.overlayY);return{main:r,fallback:{overlayX:o,overlayY:s}}}_updateTooltipMessage(){this._tooltipInstance&&(this._tooltipInstance.message=this.message,this._tooltipInstance._markForCheck(),Qe(()=>{this._tooltipInstance&&this._overlayRef.updatePosition()},{injector:this._injector}))}_setTooltipClass(e){this._tooltipInstance&&(this._tooltipInstance.tooltipClass=e instanceof Set?Array.from(e):e,this._tooltipInstance._markForCheck())}_invertPosition(e,i){return this.position==="above"||this.position==="below"?i==="top"?i="bottom":i==="bottom"&&(i="top"):e==="end"?e="start":e==="start"&&(e="end"),{x:e,y:i}}_updateCurrentPositionClass(e){let{overlayY:i,originX:r,originY:o}=e,s;if(i==="center"?this._dir&&this._dir.value==="rtl"?s=r==="end"?"left":"right":s=r==="start"?"left":"right":s=i==="bottom"&&o==="top"?"above":"below",s!==this._currentPosition){let a=this._overlayRef;if(a){let l=`${this._cssClassPrefix}-${RE}-`;a.removePanelClass(l+this._currentPosition),a.addPanelClass(l+s)}this._currentPosition=s}}_setupPointerEnterEventsIfNeeded(){this._disabled||!this.message||!this._viewInitialized||this._eventCleanups.length||(this._isTouchPlatform()?this.touchGestures!=="off"&&(this._disableNativeGesturesIfNecessary(),this._addListener("touchstart",e=>{let i=e.targetTouches?.[0],r=i?{x:i.clientX,y:i.clientY}:void 0;this._setupPointerExitEventsIfNeeded(),this._touchstartTimeout&&clearTimeout(this._touchstartTimeout);let o=500;this._touchstartTimeout=setTimeout(()=>{this._touchstartTimeout=null,this.show(void 0,r)},this._defaultOptions?.touchLongPressShowDelay??o)})):this._addListener("mouseenter",e=>{this._setupPointerExitEventsIfNeeded();let i;e.x!==void 0&&e.y!==void 0&&(i=e),this.show(void 0,i)}))}_setupPointerExitEventsIfNeeded(){if(!this._pointerExitEventsInitialized){if(this._pointerExitEventsInitialized=!0,!this._isTouchPlatform())this._addListener("mouseleave",e=>{let i=e.relatedTarget;(!i||!this._overlayRef?.overlayElement.contains(i))&&this.hide()}),this._addListener("wheel",e=>{if(this._isTooltipVisible()){let i=this._document.elementFromPoint(e.clientX,e.clientY),r=this._elementRef.nativeElement;i!==r&&!r.contains(i)&&this.hide()}});else if(this.touchGestures!=="off"){this._disableNativeGesturesIfNecessary();let e=()=>{this._touchstartTimeout&&clearTimeout(this._touchstartTimeout),this.hide(this._defaultOptions?.touchendHideDelay)};this._addListener("touchend",e),this._addListener("touchcancel",e)}}}_addListener(e,i){this._eventCleanups.push(this._renderer.listen(this._elementRef.nativeElement,e,i,DF))}_isTouchPlatform(){let e=this._defaultOptions?.detectHoverCapability;return typeof e=="function"?!e():this._platform.IOS||this._platform.ANDROID?!0:this._platform.isBrowser?!!e&&this._mediaMatcher.matchMedia("(any-hover: none)").matches:!1}_disableNativeGesturesIfNecessary(){let e=this.touchGestures;if(e!=="off"){let i=this._elementRef.nativeElement,r=i.style;(e==="on"||i.nodeName!=="INPUT"&&i.nodeName!=="TEXTAREA")&&(r.userSelect=r.msUserSelect=r.webkitUserSelect=r.MozUserSelect="none"),(e==="on"||!i.draggable)&&(r.webkitUserDrag="none"),r.touchAction="none",r.webkitTapHighlightColor="transparent"}}_syncAriaDescription(e){this._ariaDescriptionPending||(this._ariaDescriptionPending=!0,this._ariaDescriber.removeDescription(this._elementRef.nativeElement,e,"tooltip"),this._isDestroyed||Qe({write:()=>{this._ariaDescriptionPending=!1,this.message&&!this.disabled&&this._ariaDescriber.describe(this._elementRef.nativeElement,this.message,"tooltip")}},{injector:this._injector}))}_overlayEventPredicate=e=>e.type==="keydown"?this._isTooltipVisible()&&e.keyCode===27&&!ut(e):!0;static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","matTooltip",""]],hostAttrs:[1,"mat-mdc-tooltip-trigger"],hostVars:2,hostBindings:function(i,r){i&2&&j("mat-mdc-tooltip-disabled",r.disabled)},inputs:{position:[0,"matTooltipPosition","position"],positionAtOrigin:[0,"matTooltipPositionAtOrigin","positionAtOrigin"],disabled:[0,"matTooltipDisabled","disabled"],showDelay:[0,"matTooltipShowDelay","showDelay"],hideDelay:[0,"matTooltipHideDelay","hideDelay"],touchGestures:[0,"matTooltipTouchGestures","touchGestures"],message:[0,"matTooltip","message"],tooltipClass:[0,"matTooltipClass","tooltipClass"]},exportAs:["matTooltip"]})}return n})(),IF=(()=>{class n{_changeDetectorRef=u(Ae);_elementRef=u(P);_isMultiline=!1;message;tooltipClass;_showTimeoutId;_hideTimeoutId;_triggerElement;_mouseLeaveHideDelay;_animationsDisabled=Fe();_tooltip;_closeOnInteraction=!1;_isVisible=!1;_onHide=new k;_showAnimation="mat-mdc-tooltip-show";_hideAnimation="mat-mdc-tooltip-hide";constructor(){}show(e){this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=setTimeout(()=>{this._toggleVisibility(!0),this._showTimeoutId=void 0},e)}hide(e){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId=setTimeout(()=>{this._toggleVisibility(!1),this._hideTimeoutId=void 0},e)}afterHidden(){return this._onHide}isVisible(){return this._isVisible}ngOnDestroy(){this._cancelPendingAnimations(),this._onHide.complete(),this._triggerElement=null}_handleBodyInteraction(){this._closeOnInteraction&&this.hide(0)}_markForCheck(){this._changeDetectorRef.markForCheck()}_handleMouseLeave({relatedTarget:e}){(!e||!this._triggerElement.contains(e))&&(this.isVisible()?this.hide(this._mouseLeaveHideDelay):this._finalizeAnimation(!1))}_onShow(){this._isMultiline=this._isTooltipMultiline(),this._markForCheck()}_isTooltipMultiline(){let e=this._elementRef.nativeElement.getBoundingClientRect();return e.height>kF&&e.width>=SF}_handleAnimationEnd({animationName:e}){(e===this._showAnimation||e===this._hideAnimation)&&this._finalizeAnimation(e===this._showAnimation)}_cancelPendingAnimations(){this._showTimeoutId!=null&&clearTimeout(this._showTimeoutId),this._hideTimeoutId!=null&&clearTimeout(this._hideTimeoutId),this._showTimeoutId=this._hideTimeoutId=void 0}_finalizeAnimation(e){e?this._closeOnInteraction=!0:this.isVisible()||this._onHide.next()}_toggleVisibility(e){let i=this._tooltip.nativeElement,r=this._showAnimation,o=this._hideAnimation;if(i.classList.remove(e?o:r),i.classList.add(e?r:o),this._isVisible!==e&&(this._isVisible=e,this._changeDetectorRef.markForCheck()),e&&!this._animationsDisabled&&typeof getComputedStyle=="function"){let s=getComputedStyle(i);(s.getPropertyValue("animation-duration")==="0s"||s.getPropertyValue("animation-name")==="none")&&(this._animationsDisabled=!0)}e&&this._onShow(),this._animationsDisabled&&(i.classList.add("_mat-animation-noopable"),this._finalizeAnimation(e))}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-tooltip-component"]],viewQuery:function(i,r){if(i&1&&Te(bF,7),i&2){let o;G(o=W())&&(r._tooltip=o.first)}},hostAttrs:["aria-hidden","true"],hostBindings:function(i,r){i&1&&V("mouseleave",function(s){return r._handleMouseLeave(s)})},decls:4,vars:5,consts:[["tooltip",""],[1,"mdc-tooltip","mat-mdc-tooltip",3,"animationend"],[1,"mat-mdc-tooltip-surface","mdc-tooltip__surface"]],template:function(i,r){i&1&&(Ot(0,"div",1,0),Md("animationend",function(s){return r._handleAnimationEnd(s)}),Ot(2,"div",2),E(3),$t()()),i&2&&(St(r.tooltipClass),j("mdc-tooltip--multiline",r._isMultiline),y(3),It(r.message))},styles:[`.mat-mdc-tooltip {
  position: relative;
  transform: scale(0);
  display: inline-flex;
}
.mat-mdc-tooltip::before {
  content: "";
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  z-index: -1;
  position: absolute;
}
.mat-mdc-tooltip-panel-below .mat-mdc-tooltip::before {
  top: -8px;
}
.mat-mdc-tooltip-panel-above .mat-mdc-tooltip::before {
  bottom: -8px;
}
.mat-mdc-tooltip-panel-right .mat-mdc-tooltip::before {
  left: -8px;
}
.mat-mdc-tooltip-panel-left .mat-mdc-tooltip::before {
  right: -8px;
}
.mat-mdc-tooltip._mat-animation-noopable {
  animation: none;
  transform: scale(1);
}

.mat-mdc-tooltip-surface {
  word-break: normal;
  overflow-wrap: anywhere;
  padding: 4px 8px;
  min-width: 40px;
  max-width: 200px;
  min-height: 24px;
  max-height: 40vh;
  box-sizing: border-box;
  overflow: hidden;
  text-align: center;
  will-change: transform, opacity;
  background-color: var(--mat-tooltip-container-color, var(--mat-sys-inverse-surface));
  color: var(--mat-tooltip-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-tooltip-container-shape, var(--mat-sys-corner-extra-small));
  font-family: var(--mat-tooltip-supporting-text-font, var(--mat-sys-body-small-font));
  font-size: var(--mat-tooltip-supporting-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-tooltip-supporting-text-weight, var(--mat-sys-body-small-weight));
  line-height: var(--mat-tooltip-supporting-text-line-height, var(--mat-sys-body-small-line-height));
  letter-spacing: var(--mat-tooltip-supporting-text-tracking, var(--mat-sys-body-small-tracking));
}
.mat-mdc-tooltip-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 1px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}
.mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: left;
}
[dir=rtl] .mdc-tooltip--multiline .mat-mdc-tooltip-surface {
  text-align: right;
}

.mat-mdc-tooltip-panel {
  line-height: normal;
}
.mat-mdc-tooltip-panel.mat-mdc-tooltip-panel-non-interactive {
  pointer-events: none;
}

@keyframes mat-mdc-tooltip-show {
  0% {
    opacity: 0;
    transform: scale(0.8);
  }
  100% {
    opacity: 1;
    transform: scale(1);
  }
}
@keyframes mat-mdc-tooltip-hide {
  0% {
    opacity: 1;
    transform: scale(1);
  }
  100% {
    opacity: 0;
    transform: scale(0.8);
  }
}
.mat-mdc-tooltip-show {
  animation: mat-mdc-tooltip-show 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}

.mat-mdc-tooltip-hide {
  animation: mat-mdc-tooltip-hide 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}
`],encapsulation:2,changeDetection:0})}return n})();var NE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[qr,Qn,oe,qn]})}return n})();function MF(n,t){if(n&1&&(_(0,"mat-option",17),E(1),g()),n&2){let e=t.$implicit;R("value",e),y(),Ye(" ",e," ")}}function TF(n,t){if(n&1){let e=Je();_(0,"mat-form-field",14)(1,"mat-select",16,0),V("selectionChange",function(r){He(e);let o=H(2);return Ue(o._changePageSize(r.value))}),Bp(3,MF,2,2,"mat-option",17,Lp),g(),_(5,"div",18),V("click",function(){He(e);let r=dt(2);return Ue(r.open())}),g()()}if(n&2){let e=H(2);R("appearance",e._formFieldAppearance)("color",e.color),y(),R("value",e.pageSize)("disabled",e.disabled),Id("aria-labelledby",e._pageSizeLabelId),R("panelClass",e.selectConfig.panelClass||"")("disableOptionCentering",e.selectConfig.disableOptionCentering),y(2),Vp(e._displayedPageSizeOptions)}}function AF(n,t){if(n&1&&(_(0,"div",15),E(1),g()),n&2){let e=H(2);y(),It(e.pageSize)}}function RF(n,t){if(n&1&&(_(0,"div",3)(1,"div",13),E(2),g(),ce(3,TF,6,7,"mat-form-field",14),ce(4,AF,2,1,"div",15),g()),n&2){let e=H();y(),J("id",e._pageSizeLabelId),y(),Ye(" ",e._intl.itemsPerPageLabel," "),y(),de(e._displayedPageSizeOptions.length>1?3:-1),y(),de(e._displayedPageSizeOptions.length<=1?4:-1)}}function OF(n,t){if(n&1){let e=Je();_(0,"button",19),V("click",function(){He(e);let r=H();return Ue(r._buttonClicked(0,r._previousButtonsDisabled()))}),yt(),_(1,"svg",8),Z(2,"path",20),g()()}if(n&2){let e=H();R("matTooltip",e._intl.firstPageLabel)("matTooltipDisabled",e._previousButtonsDisabled())("disabled",e._previousButtonsDisabled())("tabindex",e._previousButtonsDisabled()?-1:null),J("aria-label",e._intl.firstPageLabel)}}function NF(n,t){if(n&1){let e=Je();_(0,"button",21),V("click",function(){He(e);let r=H();return Ue(r._buttonClicked(r.getNumberOfPages()-1,r._nextButtonsDisabled()))}),yt(),_(1,"svg",8),Z(2,"path",22),g()()}if(n&2){let e=H();R("matTooltip",e._intl.lastPageLabel)("matTooltipDisabled",e._nextButtonsDisabled())("disabled",e._nextButtonsDisabled())("tabindex",e._nextButtonsDisabled()?-1:null),J("aria-label",e._intl.lastPageLabel)}}var FF=(()=>{class n{changes=new k;itemsPerPageLabel="Items per page:";nextPageLabel="Next page";previousPageLabel="Previous page";firstPageLabel="First page";lastPageLabel="Last page";getRangeLabel=(e,i,r)=>{if(r==0||i==0)return`0 of ${r}`;r=Math.max(r,0);let o=e*i,s=o<r?Math.min(o+i,r):o+i;return`${o+1} \u2013 ${s} of ${r}`};static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})(),PF=50;var LF=new v("MAT_PAGINATOR_DEFAULT_OPTIONS"),fl=(()=>{class n{_intl=u(FF);_changeDetectorRef=u(Ae);_formFieldAppearance;_pageSizeLabelId=u(Ie).getId("mat-paginator-page-size-label-");_intlChanges;_isInitialized=!1;_initializedStream=new Kt(1);color;get pageIndex(){return this._pageIndex}set pageIndex(e){this._pageIndex=Math.max(e||0,0),this._changeDetectorRef.markForCheck()}_pageIndex=0;get length(){return this._length}set length(e){this._length=e||0,this._changeDetectorRef.markForCheck()}_length=0;get pageSize(){return this._pageSize}set pageSize(e){this._pageSize=Math.max(e||0,0),this._updateDisplayedPageSizeOptions()}_pageSize;get pageSizeOptions(){return this._pageSizeOptions}set pageSizeOptions(e){this._pageSizeOptions=(e||[]).map(i=>ht(i,0)),this._updateDisplayedPageSizeOptions()}_pageSizeOptions=[];hidePageSize=!1;showFirstLastButtons=!1;selectConfig={};disabled=!1;page=new z;_displayedPageSizeOptions;initialized=this._initializedStream;constructor(){let e=this._intl,i=u(LF,{optional:!0});if(this._intlChanges=e.changes.subscribe(()=>this._changeDetectorRef.markForCheck()),i){let{pageSize:r,pageSizeOptions:o,hidePageSize:s,showFirstLastButtons:a}=i;r!=null&&(this._pageSize=r),o!=null&&(this._pageSizeOptions=o),s!=null&&(this.hidePageSize=s),a!=null&&(this.showFirstLastButtons=a)}this._formFieldAppearance=i?.formFieldAppearance||"outline"}ngOnInit(){this._isInitialized=!0,this._updateDisplayedPageSizeOptions(),this._initializedStream.next()}ngOnDestroy(){this._initializedStream.complete(),this._intlChanges.unsubscribe()}nextPage(){this.hasNextPage()&&this._navigate(this.pageIndex+1)}previousPage(){this.hasPreviousPage()&&this._navigate(this.pageIndex-1)}firstPage(){this.hasPreviousPage()&&this._navigate(0)}lastPage(){this.hasNextPage()&&this._navigate(this.getNumberOfPages()-1)}hasPreviousPage(){return this.pageIndex>=1&&this.pageSize!=0}hasNextPage(){let e=this.getNumberOfPages()-1;return this.pageIndex<e&&this.pageSize!=0}getNumberOfPages(){return this.pageSize?Math.ceil(this.length/this.pageSize):0}_changePageSize(e){let i=this.pageIndex*this.pageSize,r=this.pageIndex;this.pageIndex=Math.floor(i/e)||0,this.pageSize=e,this._emitPageEvent(r)}_nextButtonsDisabled(){return this.disabled||!this.hasNextPage()}_previousButtonsDisabled(){return this.disabled||!this.hasPreviousPage()}_updateDisplayedPageSizeOptions(){this._isInitialized&&(this.pageSize||(this._pageSize=this.pageSizeOptions.length!=0?this.pageSizeOptions[0]:PF),this._displayedPageSizeOptions=this.pageSizeOptions.slice(),this._displayedPageSizeOptions.indexOf(this.pageSize)===-1&&this._displayedPageSizeOptions.push(this.pageSize),this._displayedPageSizeOptions.sort((e,i)=>e-i),this._changeDetectorRef.markForCheck())}_emitPageEvent(e){this.page.emit({previousPageIndex:e,pageIndex:this.pageIndex,pageSize:this.pageSize,length:this.length})}_navigate(e){let i=this.pageIndex;e!==i&&(this.pageIndex=e,this._emitPageEvent(i))}_buttonClicked(e,i){i||this._navigate(e)}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-paginator"]],hostAttrs:["role","group",1,"mat-mdc-paginator"],inputs:{color:"color",pageIndex:[2,"pageIndex","pageIndex",ht],length:[2,"length","length",ht],pageSize:[2,"pageSize","pageSize",ht],pageSizeOptions:"pageSizeOptions",hidePageSize:[2,"hidePageSize","hidePageSize",N],showFirstLastButtons:[2,"showFirstLastButtons","showFirstLastButtons",N],selectConfig:"selectConfig",disabled:[2,"disabled","disabled",N]},outputs:{page:"page"},exportAs:["matPaginator"],decls:14,vars:14,consts:[["selectRef",""],[1,"mat-mdc-paginator-outer-container"],[1,"mat-mdc-paginator-container"],[1,"mat-mdc-paginator-page-size"],[1,"mat-mdc-paginator-range-actions"],["aria-atomic","true","aria-live","polite","role","status",1,"mat-mdc-paginator-range-label"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-previous",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["viewBox","0 0 24 24","focusable","false","aria-hidden","true",1,"mat-mdc-paginator-icon"],["d","M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-next",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"matTooltip","matTooltipDisabled","disabled","tabindex"],["aria-hidden","true",1,"mat-mdc-paginator-page-size-label"],[1,"mat-mdc-paginator-page-size-select",3,"appearance","color"],[1,"mat-mdc-paginator-page-size-value"],["hideSingleSelectionIndicator","",3,"selectionChange","value","disabled","aria-labelledby","panelClass","disableOptionCentering"],[3,"value"],[1,"mat-mdc-paginator-touch-target",3,"click"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-first",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M18.41 16.59L13.82 12l4.59-4.59L17 6l-6 6 6 6zM6 6h2v12H6z"],["matIconButton","","type","button","matTooltipPosition","above","disabledInteractive","",1,"mat-mdc-paginator-navigation-last",3,"click","matTooltip","matTooltipDisabled","disabled","tabindex"],["d","M5.59 7.41L10.18 12l-4.59 4.59L7 18l6-6-6-6zM16 6h2v12h-2z"]],template:function(i,r){i&1&&(_(0,"div",1)(1,"div",2),ce(2,RF,5,4,"div",3),_(3,"div",4)(4,"div",5),E(5),g(),ce(6,OF,3,5,"button",6),_(7,"button",7),V("click",function(){return r._buttonClicked(r.pageIndex-1,r._previousButtonsDisabled())}),yt(),_(8,"svg",8),Z(9,"path",9),g()(),ni(),_(10,"button",10),V("click",function(){return r._buttonClicked(r.pageIndex+1,r._nextButtonsDisabled())}),yt(),_(11,"svg",8),Z(12,"path",11),g()(),ce(13,NF,3,5,"button",12),g()()()),i&2&&(y(2),de(r.hidePageSize?-1:2),y(3),Ye(" ",r._intl.getRangeLabel(r.pageIndex,r.pageSize,r.length)," "),y(),de(r.showFirstLastButtons?6:-1),y(),R("matTooltip",r._intl.previousPageLabel)("matTooltipDisabled",r._previousButtonsDisabled())("disabled",r._previousButtonsDisabled())("tabindex",r._previousButtonsDisabled()?-1:null),J("aria-label",r._intl.previousPageLabel),y(3),R("matTooltip",r._intl.nextPageLabel)("matTooltipDisabled",r._nextButtonsDisabled())("disabled",r._nextButtonsDisabled())("tabindex",r._nextButtonsDisabled()?-1:null),J("aria-label",r._intl.nextPageLabel),y(3),de(r.showFirstLastButtons?13:-1))},dependencies:[Wn,TE,gs,D_,OE],styles:[`.mat-mdc-paginator {
  display: block;
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  color: var(--mat-paginator-container-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-paginator-container-background-color, var(--mat-sys-surface));
  font-family: var(--mat-paginator-container-text-font, var(--mat-sys-body-small-font));
  line-height: var(--mat-paginator-container-text-line-height, var(--mat-sys-body-small-line-height));
  font-size: var(--mat-paginator-container-text-size, var(--mat-sys-body-small-size));
  font-weight: var(--mat-paginator-container-text-weight, var(--mat-sys-body-small-weight));
  letter-spacing: var(--mat-paginator-container-text-tracking, var(--mat-sys-body-small-tracking));
  --mat-form-field-container-height: var(--mat-paginator-form-field-container-height, 40px);
  --mat-form-field-container-vertical-padding: var(--mat-paginator-form-field-container-vertical-padding, 8px);
}
.mat-mdc-paginator .mat-mdc-select-value {
  font-size: var(--mat-paginator-select-trigger-text-size, var(--mat-sys-body-small-size));
}
.mat-mdc-paginator .mat-mdc-form-field-subscript-wrapper {
  display: none;
}
.mat-mdc-paginator .mat-mdc-select {
  line-height: 1.5;
}

.mat-mdc-paginator-outer-container {
  display: flex;
}

.mat-mdc-paginator-container {
  display: flex;
  align-items: center;
  justify-content: flex-end;
  padding: 0 8px;
  flex-wrap: wrap;
  width: 100%;
  min-height: var(--mat-paginator-container-size, 56px);
}

.mat-mdc-paginator-page-size {
  display: flex;
  align-items: baseline;
  margin-right: 8px;
}
[dir=rtl] .mat-mdc-paginator-page-size {
  margin-right: 0;
  margin-left: 8px;
}

.mat-mdc-paginator-page-size-label {
  margin: 0 4px;
}

.mat-mdc-paginator-page-size-select {
  margin: 0 4px;
  width: var(--mat-paginator-page-size-select-width, 84px);
}

.mat-mdc-paginator-range-label {
  margin: 0 32px 0 24px;
}

.mat-mdc-paginator-range-actions {
  display: flex;
  align-items: center;
}

.mat-mdc-paginator-icon {
  display: inline-block;
  width: 28px;
  fill: var(--mat-paginator-enabled-icon-color, var(--mat-sys-on-surface-variant));
}
.mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon {
  fill: var(--mat-paginator-disabled-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
[dir=rtl] .mat-mdc-paginator-icon {
  transform: rotate(180deg);
}

@media (forced-colors: active) {
  .mat-mdc-icon-button[aria-disabled] .mat-mdc-paginator-icon,
  .mat-mdc-paginator-icon {
    fill: currentColor;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button {
    outline: solid 1px;
  }
  .mat-mdc-paginator-range-actions .mat-mdc-icon-button[aria-disabled] {
    color: GrayText;
  }
}
.mat-mdc-paginator-touch-target {
  display: var(--mat-paginator-touch-target-display, block);
  position: absolute;
  top: 50%;
  left: 50%;
  width: var(--mat-paginator-page-size-select-width, 84px);
  height: var(--mat-paginator-page-size-select-touch-target-height, 48px);
  background-color: transparent;
  transform: translate(-50%, -50%);
  cursor: pointer;
}
`],encapsulation:2,changeDetection:0})}return n})(),FE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[Ki,AE,NE,fl]})}return n})();var VF=[[["caption"]],[["colgroup"],["col"]],"*"],jF=["caption","colgroup, col","*"];function zF(n,t){n&1&&ie(0,2)}function HF(n,t){n&1&&(_(0,"thead",0),nt(1,1),g(),_(2,"tbody",0),nt(3,2)(4,3),g(),_(5,"tfoot",0),nt(6,4),g())}function UF(n,t){n&1&&nt(0,1)(1,2)(2,3)(3,4)}var Mn=new v("CDK_TABLE");var dm=(()=>{class n{template=u(Pe);constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkCellDef",""]]})}return n})(),um=(()=>{class n{template=u(Pe);constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkHeaderCellDef",""]]})}return n})(),BE=(()=>{class n{template=u(Pe);constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkFooterCellDef",""]]})}return n})(),_s=(()=>{class n{_table=u(Mn,{optional:!0});_hasStickyChanged=!1;get name(){return this._name}set name(e){this._setNameInput(e)}_name;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;get stickyEnd(){return this._stickyEnd}set stickyEnd(e){e!==this._stickyEnd&&(this._stickyEnd=e,this._hasStickyChanged=!0)}_stickyEnd=!1;cell;headerCell;footerCell;cssClassFriendlyName;_columnCssClassName;constructor(){}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}_updateColumnCssClassName(){this._columnCssClassName=[`cdk-column-${this.cssClassFriendlyName}`]}_setNameInput(e){e&&(this._name=e,this.cssClassFriendlyName=e.replace(/[^a-z0-9_-]/gi,"-"),this._updateColumnCssClassName())}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkColumnDef",""]],contentQueries:function(i,r,o){if(i&1&&kt(o,dm,5)(o,um,5)(o,BE,5),i&2){let s;G(s=W())&&(r.cell=s.first),G(s=W())&&(r.headerCell=s.first),G(s=W())&&(r.footerCell=s.first)}},inputs:{name:[0,"cdkColumnDef","name"],sticky:[2,"sticky","sticky",N],stickyEnd:[2,"stickyEnd","stickyEnd",N]}})}return n})(),cm=class{constructor(t,e){e.nativeElement.classList.add(...t._columnCssClassName)}},VE=(()=>{class n extends cm{constructor(){super(u(_s),u(P))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["cdk-header-cell"],["th","cdk-header-cell",""]],hostAttrs:["role","columnheader",1,"cdk-header-cell"],features:[re]})}return n})();var jE=(()=>{class n extends cm{constructor(){let e=u(_s),i=u(P);super(e,i);let r=e._table?._getCellRole();r&&i.nativeElement.setAttribute("role",r)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["cdk-cell"],["td","cdk-cell",""]],hostAttrs:[1,"cdk-cell"],features:[re]})}return n})();var V_=(()=>{class n{template=u(Pe);_differs=u(si);columns;_columnsDiffer;constructor(){}ngOnChanges(e){if(!this._columnsDiffer){let i=e.columns&&e.columns.currentValue||[];this._columnsDiffer=this._differs.find(i).create(),this._columnsDiffer.diff(i)}}getColumnsDiff(){return this._columnsDiffer.diff(this.columns)}extractCellTemplate(e){return this instanceof gl?e.headerCell.template:this instanceof j_?e.footerCell.template:e.cell.template}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,features:[Le]})}return n})(),gl=(()=>{class n extends V_{_table=u(Mn,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(u(Pe),u(si))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkHeaderRowDef",""]],inputs:{columns:[0,"cdkHeaderRowDef","columns"],sticky:[2,"cdkHeaderRowDefSticky","sticky",N]},features:[re,Le]})}return n})(),j_=(()=>{class n extends V_{_table=u(Mn,{optional:!0});_hasStickyChanged=!1;get sticky(){return this._sticky}set sticky(e){e!==this._sticky&&(this._sticky=e,this._hasStickyChanged=!0)}_sticky=!1;constructor(){super(u(Pe),u(si))}ngOnChanges(e){super.ngOnChanges(e)}hasStickyChanged(){let e=this._hasStickyChanged;return this.resetStickyChanged(),e}resetStickyChanged(){this._hasStickyChanged=!1}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkFooterRowDef",""]],inputs:{columns:[0,"cdkFooterRowDef","columns"],sticky:[2,"cdkFooterRowDefSticky","sticky",N]},features:[re,Le]})}return n})(),mm=(()=>{class n extends V_{_table=u(Mn,{optional:!0});when;constructor(){super(u(Pe),u(si))}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkRowDef",""]],inputs:{columns:[0,"cdkRowDefColumns","columns"],when:[0,"cdkRowDefWhen","when"]},features:[re]})}return n})(),eo=(()=>{class n{_viewContainer=u(tt);cells;context;static mostRecentCellOutlet=null;constructor(){n.mostRecentCellOutlet=this}ngOnDestroy(){n.mostRecentCellOutlet===this&&(n.mostRecentCellOutlet=null)}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","cdkCellOutlet",""]]})}return n})(),z_=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["cdk-header-row"],["tr","cdk-header-row",""]],hostAttrs:["role","row",1,"cdk-header-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&nt(0,0)},dependencies:[eo],encapsulation:2})}return n})();var H_=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["cdk-row"],["tr","cdk-row",""]],hostAttrs:["role","row",1,"cdk-row"],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&nt(0,0)},dependencies:[eo],encapsulation:2})}return n})(),zE=(()=>{class n{templateRef=u(Pe);_contentClassNames=["cdk-no-data-row","cdk-row"];_cellClassNames=["cdk-cell","cdk-no-data-cell"];_cellSelector="td, cdk-cell, [cdk-cell], .cdk-cell";constructor(){}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["ng-template","cdkNoDataRow",""]]})}return n})(),PE=["top","bottom","left","right"],B_=class{_isNativeHtmlTable;_stickCellCss;_isBrowser;_needsPositionStickyOnElement;direction;_positionListener;_tableInjector;_elemSizeCache=new WeakMap;_resizeObserver=globalThis?.ResizeObserver?new globalThis.ResizeObserver(t=>this._updateCachedSizes(t)):null;_updatedStickyColumnsParamsToReplay=[];_stickyColumnsReplayTimeout=null;_cachedCellWidths=[];_borderCellCss;_destroyed=!1;constructor(t,e,i=!0,r=!0,o,s,a){this._isNativeHtmlTable=t,this._stickCellCss=e,this._isBrowser=i,this._needsPositionStickyOnElement=r,this.direction=o,this._positionListener=s,this._tableInjector=a,this._borderCellCss={top:`${e}-border-elem-top`,bottom:`${e}-border-elem-bottom`,left:`${e}-border-elem-left`,right:`${e}-border-elem-right`}}clearStickyPositioning(t,e){(e.includes("left")||e.includes("right"))&&this._removeFromStickyColumnReplayQueue(t);let i=[];for(let r of t)r.nodeType===r.ELEMENT_NODE&&i.push(r,...Array.from(r.children));Qe({write:()=>{for(let r of i)this._removeStickyStyle(r,e)}},{injector:this._tableInjector})}updateStickyColumns(t,e,i,r=!0,o=!0){if(!t.length||!this._isBrowser||!(e.some(C=>C)||i.some(C=>C))){this._positionListener?.stickyColumnsUpdated({sizes:[]}),this._positionListener?.stickyEndColumnsUpdated({sizes:[]});return}let s=t[0],a=s.children.length,l=this.direction==="rtl",c=l?"right":"left",d=l?"left":"right",m=e.lastIndexOf(!0),f=i.indexOf(!0),h,p,b;o&&this._updateStickyColumnReplayQueue({rows:[...t],stickyStartStates:[...e],stickyEndStates:[...i]}),Qe({earlyRead:()=>{h=this._getCellWidths(s,r),p=this._getStickyStartColumnPositions(h,e),b=this._getStickyEndColumnPositions(h,i)},write:()=>{for(let C of t)for(let x=0;x<a;x++){let ee=C.children[x];e[x]&&this._addStickyStyle(ee,c,p[x],x===m),i[x]&&this._addStickyStyle(ee,d,b[x],x===f)}this._positionListener&&h.some(C=>!!C)&&(this._positionListener.stickyColumnsUpdated({sizes:m===-1?[]:h.slice(0,m+1).map((C,x)=>e[x]?C:null)}),this._positionListener.stickyEndColumnsUpdated({sizes:f===-1?[]:h.slice(f).map((C,x)=>i[x+f]?C:null).reverse()}))}},{injector:this._tableInjector})}stickRows(t,e,i){if(!this._isBrowser)return;let r=i==="bottom"?t.slice().reverse():t,o=i==="bottom"?e.slice().reverse():e,s=[],a=[],l=[];Qe({earlyRead:()=>{for(let c=0,d=0;c<r.length;c++){if(!o[c])continue;s[c]=d;let m=r[c];l[c]=this._isNativeHtmlTable?Array.from(m.children):[m];let f=this._retrieveElementSize(m).height;d+=f,a[c]=f}},write:()=>{let c=o.lastIndexOf(!0);for(let d=0;d<r.length;d++){if(!o[d])continue;let m=s[d],f=d===c;for(let h of l[d])this._addStickyStyle(h,i,m,f)}i==="top"?this._positionListener?.stickyHeaderRowsUpdated({sizes:a,offsets:s,elements:l}):this._positionListener?.stickyFooterRowsUpdated({sizes:a,offsets:s,elements:l})}},{injector:this._tableInjector})}updateStickyFooterContainer(t,e){this._isNativeHtmlTable&&Qe({write:()=>{let i=t.querySelector("tfoot");i&&(e.some(r=>!r)?this._removeStickyStyle(i,["bottom"]):this._addStickyStyle(i,"bottom",0,!1))}},{injector:this._tableInjector})}destroy(){this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._resizeObserver?.disconnect(),this._destroyed=!0}_removeStickyStyle(t,e){if(!t.classList.contains(this._stickCellCss))return;for(let r of e)t.style[r]="",t.classList.remove(this._borderCellCss[r]);PE.some(r=>e.indexOf(r)===-1&&t.style[r])?t.style.zIndex=this._getCalculatedZIndex(t):(t.style.zIndex="",this._needsPositionStickyOnElement&&(t.style.position=""),t.classList.remove(this._stickCellCss))}_addStickyStyle(t,e,i,r){t.classList.add(this._stickCellCss),r&&t.classList.add(this._borderCellCss[e]),t.style[e]=`${i}px`,t.style.zIndex=this._getCalculatedZIndex(t),this._needsPositionStickyOnElement&&(t.style.cssText+="position: -webkit-sticky; position: sticky; ")}_getCalculatedZIndex(t){let e={top:100,bottom:10,left:1,right:1},i=0;for(let r of PE)t.style[r]&&(i+=e[r]);return i?`${i}`:""}_getCellWidths(t,e=!0){if(!e&&this._cachedCellWidths.length)return this._cachedCellWidths;let i=[],r=t.children;for(let o=0;o<r.length;o++){let s=r[o];i.push(this._retrieveElementSize(s).width)}return this._cachedCellWidths=i,i}_getStickyStartColumnPositions(t,e){let i=[],r=0;for(let o=0;o<t.length;o++)e[o]&&(i[o]=r,r+=t[o]);return i}_getStickyEndColumnPositions(t,e){let i=[],r=0;for(let o=t.length;o>0;o--)e[o]&&(i[o]=r,r+=t[o]);return i}_retrieveElementSize(t){let e=this._elemSizeCache.get(t);if(e)return e;let i=t.getBoundingClientRect(),r={width:i.width,height:i.height};return this._resizeObserver&&(this._elemSizeCache.set(t,r),this._resizeObserver.observe(t,{box:"border-box"})),r}_updateStickyColumnReplayQueue(t){this._removeFromStickyColumnReplayQueue(t.rows),this._stickyColumnsReplayTimeout||this._updatedStickyColumnsParamsToReplay.push(t)}_removeFromStickyColumnReplayQueue(t){let e=new Set(t);for(let i of this._updatedStickyColumnsParamsToReplay)i.rows=i.rows.filter(r=>!e.has(r));this._updatedStickyColumnsParamsToReplay=this._updatedStickyColumnsParamsToReplay.filter(i=>!!i.rows.length)}_updateCachedSizes(t){let e=!1;for(let i of t){let r=i.borderBoxSize?.length?{width:i.borderBoxSize[0].inlineSize,height:i.borderBoxSize[0].blockSize}:{width:i.contentRect.width,height:i.contentRect.height};r.width!==this._elemSizeCache.get(i.target)?.width&&$F(i.target)&&(e=!0),this._elemSizeCache.set(i.target,r)}e&&this._updatedStickyColumnsParamsToReplay.length&&(this._stickyColumnsReplayTimeout&&clearTimeout(this._stickyColumnsReplayTimeout),this._stickyColumnsReplayTimeout=setTimeout(()=>{if(!this._destroyed){for(let i of this._updatedStickyColumnsParamsToReplay)this.updateStickyColumns(i.rows,i.stickyStartStates,i.stickyEndStates,!0,!1);this._updatedStickyColumnsParamsToReplay=[],this._stickyColumnsReplayTimeout=null}},0))}};function $F(n){return["cdk-cell","cdk-header-cell","cdk-footer-cell"].some(t=>n.classList.contains(t))}var pl=new v("STICKY_POSITIONING_LISTENER");var U_=(()=>{class n{viewContainer=u(tt);elementRef=u(P);constructor(){let e=u(Mn);e._rowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","rowOutlet",""]]})}return n})(),$_=(()=>{class n{viewContainer=u(tt);elementRef=u(P);constructor(){let e=u(Mn);e._headerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","headerRowOutlet",""]]})}return n})(),G_=(()=>{class n{viewContainer=u(tt);elementRef=u(P);constructor(){let e=u(Mn);e._footerRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","footerRowOutlet",""]]})}return n})(),W_=(()=>{class n{viewContainer=u(tt);elementRef=u(P);constructor(){let e=u(Mn);e._noDataRowOutlet=this,e._outletAssigned()}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","noDataRowOutlet",""]]})}return n})(),q_=(()=>{class n{_differs=u(si);_changeDetectorRef=u(Ae);_elementRef=u(P);_dir=u(st,{optional:!0});_platform=u(_e);_viewRepeater;_viewportRuler=u(Sn);_injector=u($);_virtualScrollViewport=u(uE,{optional:!0,host:!0});_positionListener=u(pl,{optional:!0})||u(pl,{optional:!0,skipSelf:!0});_document=u(U);_data;_renderedRange;_onDestroy=new k;_renderRows;_renderChangeSubscription=null;_columnDefsByName=new Map;_rowDefs;_headerRowDefs;_footerRowDefs;_dataDiffer;_defaultRowDef=null;_customColumnDefs=new Set;_customRowDefs=new Set;_customHeaderRowDefs=new Set;_customFooterRowDefs=new Set;_customNoDataRow=null;_headerRowDefChanged=!0;_footerRowDefChanged=!0;_stickyColumnStylesNeedReset=!0;_forceRecalculateCellWidths=!0;_cachedRenderRowsMap=new Map;_isNativeHtmlTable;_stickyStyler;stickyCssClass="cdk-table-sticky";needsPositionStickyOnElement=!0;_isServer;_isShowingNoDataRow=!1;_hasAllOutlets=!1;_hasInitialized=!1;_headerRowStickyUpdates=new k;_footerRowStickyUpdates=new k;_disableVirtualScrolling=!1;_getCellRole(){if(this._cellRoleInternal===void 0){let e=this._elementRef.nativeElement.getAttribute("role");return e==="grid"||e==="treegrid"?"gridcell":"cell"}return this._cellRoleInternal}_cellRoleInternal=void 0;get trackBy(){return this._trackByFn}set trackBy(e){this._trackByFn=e}_trackByFn;get dataSource(){return this._dataSource}set dataSource(e){this._dataSource!==e&&(this._switchDataSource(e),this._changeDetectorRef.markForCheck())}_dataSource;_dataSourceChanges=new k;_dataStream=new k;get multiTemplateDataRows(){return this._multiTemplateDataRows}set multiTemplateDataRows(e){this._multiTemplateDataRows=e,this._rowOutlet&&this._rowOutlet.viewContainer.length&&(this._forceRenderDataRows(),this.updateStickyColumnStyles())}_multiTemplateDataRows=!1;get fixedLayout(){return this._virtualScrollEnabled()?!0:this._fixedLayout}set fixedLayout(e){this._fixedLayout=e,this._forceRecalculateCellWidths=!0,this._stickyColumnStylesNeedReset=!0}_fixedLayout=!1;recycleRows=!1;contentChanged=new z;viewChange=new Bt({start:0,end:Number.MAX_VALUE});_rowOutlet;_headerRowOutlet;_footerRowOutlet;_noDataRowOutlet;_contentColumnDefs;_contentRowDefs;_contentHeaderRowDefs;_contentFooterRowDefs;_noDataRow;constructor(){u(new an("role"),{optional:!0})||this._elementRef.nativeElement.setAttribute("role","table"),this._isServer=!this._platform.isBrowser,this._isNativeHtmlTable=this._elementRef.nativeElement.nodeName==="TABLE",this._dataDiffer=this._differs.find([]).create((i,r)=>this.trackBy?this.trackBy(r.dataIndex,r.data):r)}ngOnInit(){this._setupStickyStyler(),this._viewportRuler.change().pipe(Ce(this._onDestroy)).subscribe(()=>{this._forceRecalculateCellWidths=!0})}ngAfterContentInit(){this._viewRepeater=this.recycleRows||this._virtualScrollEnabled()?new Ku:new Yu,this._virtualScrollEnabled()&&this._setupVirtualScrolling(this._virtualScrollViewport),this._hasInitialized=!0}ngAfterContentChecked(){this._canRender()&&this._render()}ngOnDestroy(){this._stickyStyler?.destroy(),[this._rowOutlet?.viewContainer,this._headerRowOutlet?.viewContainer,this._footerRowOutlet?.viewContainer,this._cachedRenderRowsMap,this._customColumnDefs,this._customRowDefs,this._customHeaderRowDefs,this._customFooterRowDefs,this._columnDefsByName].forEach(e=>{e?.clear()}),this._headerRowDefs=[],this._footerRowDefs=[],this._defaultRowDef=null,this._headerRowStickyUpdates.complete(),this._footerRowStickyUpdates.complete(),this._onDestroy.next(),this._onDestroy.complete(),ll(this.dataSource)&&this.dataSource.disconnect(this)}renderRows(){this._renderRows=this._getAllRenderRows();let e=this._dataDiffer.diff(this._renderRows);if(!e){this._updateNoDataRow(),this.contentChanged.next();return}let i=this._rowOutlet.viewContainer;this._viewRepeater.applyChanges(e,i,(r,o,s)=>this._getEmbeddedViewArgs(r.item,s),r=>r.item.data,r=>{r.operation===kn.INSERTED&&r.context&&this._renderCellTemplateForItem(r.record.item.rowDef,r.context)}),this._updateRowIndexContext(),e.forEachIdentityChange(r=>{let o=i.get(r.currentIndex);o.context.$implicit=r.item.data}),this._updateNoDataRow(),this.contentChanged.next(),this.updateStickyColumnStyles()}addColumnDef(e){this._customColumnDefs.add(e)}removeColumnDef(e){this._customColumnDefs.delete(e)}addRowDef(e){this._customRowDefs.add(e)}removeRowDef(e){this._customRowDefs.delete(e)}addHeaderRowDef(e){this._customHeaderRowDefs.add(e),this._headerRowDefChanged=!0}removeHeaderRowDef(e){this._customHeaderRowDefs.delete(e),this._headerRowDefChanged=!0}addFooterRowDef(e){this._customFooterRowDefs.add(e),this._footerRowDefChanged=!0}removeFooterRowDef(e){this._customFooterRowDefs.delete(e),this._footerRowDefChanged=!0}setNoDataRow(e){this._customNoDataRow=e}updateStickyHeaderRowStyles(){let e=this._getRenderedRows(this._headerRowOutlet);if(this._isNativeHtmlTable){let r=LE(this._headerRowOutlet,"thead");r&&(r.style.display=e.length?"":"none")}let i=this._headerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["top"]),this._stickyStyler.stickRows(e,i,"top"),this._headerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyFooterRowStyles(){let e=this._getRenderedRows(this._footerRowOutlet);if(this._isNativeHtmlTable){let r=LE(this._footerRowOutlet,"tfoot");r&&(r.style.display=e.length?"":"none")}let i=this._footerRowDefs.map(r=>r.sticky);this._stickyStyler.clearStickyPositioning(e,["bottom"]),this._stickyStyler.stickRows(e,i,"bottom"),this._stickyStyler.updateStickyFooterContainer(this._elementRef.nativeElement,i),this._footerRowDefs.forEach(r=>r.resetStickyChanged())}updateStickyColumnStyles(){let e=this._getRenderedRows(this._headerRowOutlet),i=this._getRenderedRows(this._rowOutlet),r=this._getRenderedRows(this._footerRowOutlet);(this._isNativeHtmlTable&&!this.fixedLayout||this._stickyColumnStylesNeedReset)&&(this._stickyStyler.clearStickyPositioning([...e,...i,...r],["left","right"]),this._stickyColumnStylesNeedReset=!1),e.forEach((o,s)=>{this._addStickyColumnStyles([o],this._headerRowDefs[s])}),this._rowDefs.forEach(o=>{let s=[];for(let a=0;a<i.length;a++)this._renderRows[a].rowDef===o&&s.push(i[a]);this._addStickyColumnStyles(s,o)}),r.forEach((o,s)=>{this._addStickyColumnStyles([o],this._footerRowDefs[s])}),Array.from(this._columnDefsByName.values()).forEach(o=>o.resetStickyChanged())}stickyColumnsUpdated(e){this._positionListener?.stickyColumnsUpdated(e)}stickyEndColumnsUpdated(e){this._positionListener?.stickyEndColumnsUpdated(e)}stickyHeaderRowsUpdated(e){this._headerRowStickyUpdates.next(e),this._positionListener?.stickyHeaderRowsUpdated(e)}stickyFooterRowsUpdated(e){this._footerRowStickyUpdates.next(e),this._positionListener?.stickyFooterRowsUpdated(e)}_outletAssigned(){!this._hasAllOutlets&&this._rowOutlet&&this._headerRowOutlet&&this._footerRowOutlet&&this._noDataRowOutlet&&(this._hasAllOutlets=!0,this._canRender()&&this._render())}_canRender(){return this._hasAllOutlets&&this._hasInitialized}_render(){this._cacheRowDefs(),this._cacheColumnDefs(),!this._headerRowDefs.length&&!this._footerRowDefs.length&&this._rowDefs.length;let i=this._renderUpdatedColumns()||this._headerRowDefChanged||this._footerRowDefChanged;this._stickyColumnStylesNeedReset=this._stickyColumnStylesNeedReset||i,this._forceRecalculateCellWidths=i,this._headerRowDefChanged&&(this._forceRenderHeaderRows(),this._headerRowDefChanged=!1),this._footerRowDefChanged&&(this._forceRenderFooterRows(),this._footerRowDefChanged=!1),this.dataSource&&this._rowDefs.length>0&&!this._renderChangeSubscription?this._observeRenderChanges():this._stickyColumnStylesNeedReset&&this.updateStickyColumnStyles(),this._checkStickyStates()}_getAllRenderRows(){if(!Array.isArray(this._data)||!this._renderedRange)return[];let e=[],i=Math.min(this._data.length,this._renderedRange.end),r=this._cachedRenderRowsMap;this._cachedRenderRowsMap=new Map;for(let o=this._renderedRange.start;o<i;o++){let s=this._data[o],a=this._getRenderRowsForData(s,o,r.get(s));this._cachedRenderRowsMap.has(s)||this._cachedRenderRowsMap.set(s,new WeakMap);for(let l=0;l<a.length;l++){let c=a[l],d=this._cachedRenderRowsMap.get(c.data);d.has(c.rowDef)?d.get(c.rowDef).push(c):d.set(c.rowDef,[c]),e.push(c)}}return e}_getRenderRowsForData(e,i,r){return this._getRowDefs(e,i).map(s=>{let a=r&&r.has(s)?r.get(s):[];if(a.length){let l=a.shift();return l.dataIndex=i,l}else return{data:e,rowDef:s,dataIndex:i}})}_cacheColumnDefs(){this._columnDefsByName.clear(),lm(this._getOwnDefs(this._contentColumnDefs),this._customColumnDefs).forEach(i=>{this._columnDefsByName.has(i.name),this._columnDefsByName.set(i.name,i)})}_cacheRowDefs(){this._headerRowDefs=lm(this._getOwnDefs(this._contentHeaderRowDefs),this._customHeaderRowDefs),this._footerRowDefs=lm(this._getOwnDefs(this._contentFooterRowDefs),this._customFooterRowDefs),this._rowDefs=lm(this._getOwnDefs(this._contentRowDefs),this._customRowDefs);let e=this._rowDefs.filter(i=>!i.when);this._defaultRowDef=e[0]}_renderUpdatedColumns(){let e=(s,a)=>{let l=!!a.getColumnsDiff();return s||l},i=this._rowDefs.reduce(e,!1);i&&this._forceRenderDataRows();let r=this._headerRowDefs.reduce(e,!1);r&&this._forceRenderHeaderRows();let o=this._footerRowDefs.reduce(e,!1);return o&&this._forceRenderFooterRows(),i||r||o}_switchDataSource(e){this._data=[],ll(this.dataSource)&&this.dataSource.disconnect(this),this._renderChangeSubscription&&(this._renderChangeSubscription.unsubscribe(),this._renderChangeSubscription=null),e||(this._dataDiffer&&this._dataDiffer.diff([]),this._rowOutlet&&this._rowOutlet.viewContainer.clear()),this._dataSource=e}_observeRenderChanges(){if(!this.dataSource)return;let e;ll(this.dataSource)?e=this.dataSource.connect(this):ac(this.dataSource)?e=this.dataSource:Array.isArray(this.dataSource)&&(e=$e(this.dataSource)),this._renderChangeSubscription=hn([e,this.viewChange]).pipe(Ce(this._onDestroy)).subscribe(([i,r])=>{this._data=i||[],this._renderedRange=r,this._dataStream.next(i),this.renderRows()})}_forceRenderHeaderRows(){this._headerRowOutlet.viewContainer.length>0&&this._headerRowOutlet.viewContainer.clear(),this._headerRowDefs.forEach((e,i)=>this._renderRow(this._headerRowOutlet,e,i)),this.updateStickyHeaderRowStyles()}_forceRenderFooterRows(){this._footerRowOutlet.viewContainer.length>0&&this._footerRowOutlet.viewContainer.clear(),this._footerRowDefs.forEach((e,i)=>this._renderRow(this._footerRowOutlet,e,i)),this.updateStickyFooterRowStyles()}_addStickyColumnStyles(e,i){let r=Array.from(i?.columns||[]).map(a=>{let l=this._columnDefsByName.get(a);return l}),o=r.map(a=>a.sticky),s=r.map(a=>a.stickyEnd);this._stickyStyler.updateStickyColumns(e,o,s,!this.fixedLayout||this._forceRecalculateCellWidths)}_getRenderedRows(e){let i=[];for(let r=0;r<e.viewContainer.length;r++){let o=e.viewContainer.get(r);i.push(o.rootNodes[0])}return i}_getRowDefs(e,i){if(this._rowDefs.length===1)return[this._rowDefs[0]];let r=[];if(this.multiTemplateDataRows)r=this._rowDefs.filter(o=>!o.when||o.when(i,e));else{let o=this._rowDefs.find(s=>s.when&&s.when(i,e))||this._defaultRowDef;o&&r.push(o)}return r.length,r}_getEmbeddedViewArgs(e,i){let r=e.rowDef,o={$implicit:e.data};return{templateRef:r.template,context:o,index:i}}_renderRow(e,i,r,o={}){let s=e.viewContainer.createEmbeddedView(i.template,o,r);return this._renderCellTemplateForItem(i,o),s}_renderCellTemplateForItem(e,i){for(let r of this._getCellTemplates(e))eo.mostRecentCellOutlet&&eo.mostRecentCellOutlet._viewContainer.createEmbeddedView(r,i);this._changeDetectorRef.markForCheck()}_updateRowIndexContext(){let e=this._rowOutlet.viewContainer;for(let i=0,r=e.length;i<r;i++){let s=e.get(i).context;s.count=r,s.first=i===0,s.last=i===r-1,s.even=i%2===0,s.odd=!s.even,this.multiTemplateDataRows?(s.dataIndex=this._renderRows[i].dataIndex,s.renderIndex=i):s.index=this._renderRows[i].dataIndex}}_getCellTemplates(e){return!e||!e.columns?[]:Array.from(e.columns,i=>{let r=this._columnDefsByName.get(i);return e.extractCellTemplate(r)})}_forceRenderDataRows(){this._dataDiffer.diff([]),this._rowOutlet.viewContainer.clear(),this.renderRows()}_checkStickyStates(){let e=(i,r)=>i||r.hasStickyChanged();this._headerRowDefs.reduce(e,!1)&&this.updateStickyHeaderRowStyles(),this._footerRowDefs.reduce(e,!1)&&this.updateStickyFooterRowStyles(),Array.from(this._columnDefsByName.values()).reduce(e,!1)&&(this._stickyColumnStylesNeedReset=!0,this.updateStickyColumnStyles())}_setupStickyStyler(){let e=this._dir?this._dir.value:"ltr",i=this._injector;this._stickyStyler=new B_(this._isNativeHtmlTable,this.stickyCssClass,this._platform.isBrowser,this.needsPositionStickyOnElement,e,this,i),(this._dir?this._dir.change:$e()).pipe(Ce(this._onDestroy)).subscribe(r=>{this._stickyStyler.direction=r,this.updateStickyColumnStyles()})}_setupVirtualScrolling(e){let i=typeof requestAnimationFrame<"u"?Wl:Ul;this.viewChange.next({start:0,end:0}),e.renderedRangeStream.pipe(ho(0,i),Ce(this._onDestroy)).subscribe(this.viewChange),e.attach({dataStream:this._dataStream,measureRangeSize:(r,o)=>this._measureRangeSize(r,o)}),hn([e.renderedContentOffset,this._headerRowStickyUpdates]).pipe(Ce(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a){let l=o.offsets[s],c=r!==0?Math.max(r-l,l):-l;for(let d of a)d.style.top=`${-c}px`}}}),hn([e.renderedContentOffset,this._footerRowStickyUpdates]).pipe(Ce(this._onDestroy)).subscribe(([r,o])=>{if(!(!o.sizes||!o.offsets||!o.elements))for(let s=0;s<o.elements.length;s++){let a=o.elements[s];if(a)for(let l of a)l.style.bottom=`${r+o.offsets[s]}px`}})}_getOwnDefs(e){return e.filter(i=>!i._table||i._table===this)}_updateNoDataRow(){let e=this._customNoDataRow||this._noDataRow;if(!e)return;let i=this._rowOutlet.viewContainer.length===0;if(i===this._isShowingNoDataRow)return;let r=this._noDataRowOutlet.viewContainer;if(i){let o=r.createEmbeddedView(e.templateRef),s=o.rootNodes[0];if(o.rootNodes.length===1&&s?.nodeType===this._document.ELEMENT_NODE){s.setAttribute("role","row"),s.classList.add(...e._contentClassNames);let a=s.querySelectorAll(e._cellSelector);for(let l=0;l<a.length;l++)a[l].classList.add(...e._cellClassNames)}}else r.clear();this._isShowingNoDataRow=i,this._changeDetectorRef.markForCheck()}_measureRangeSize(e,i){if(e.start>=e.end||i!=="vertical")return 0;let r=this.viewChange.value,o=this._rowOutlet.viewContainer;e.start<r.start||e.end>r.end;let s=e.start-r.start,a=e.end-e.start,l,c;for(let f=0;f<a;f++){let h=o.get(f+s);if(h&&h.rootNodes.length){l=c=h.rootNodes[0];break}}for(let f=a-1;f>-1;f--){let h=o.get(f+s);if(h&&h.rootNodes.length){c=h.rootNodes[h.rootNodes.length-1];break}}let d=l?.getBoundingClientRect?.(),m=c?.getBoundingClientRect?.();return d&&m?m.bottom-d.top:0}_virtualScrollEnabled(){return!this._disableVirtualScrolling&&this._virtualScrollViewport!=null}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["cdk-table"],["table","cdk-table",""]],contentQueries:function(i,r,o){if(i&1&&kt(o,zE,5)(o,_s,5)(o,mm,5)(o,gl,5)(o,j_,5),i&2){let s;G(s=W())&&(r._noDataRow=s.first),G(s=W())&&(r._contentColumnDefs=s),G(s=W())&&(r._contentRowDefs=s),G(s=W())&&(r._contentHeaderRowDefs=s),G(s=W())&&(r._contentFooterRowDefs=s)}},hostAttrs:[1,"cdk-table"],hostVars:2,hostBindings:function(i,r){i&2&&j("cdk-table-fixed-layout",r.fixedLayout)},inputs:{trackBy:"trackBy",dataSource:"dataSource",multiTemplateDataRows:[2,"multiTemplateDataRows","multiTemplateDataRows",N],fixedLayout:[2,"fixedLayout","fixedLayout",N],recycleRows:[2,"recycleRows","recycleRows",N]},outputs:{contentChanged:"contentChanged"},exportAs:["cdkTable"],features:[ve([{provide:Mn,useExisting:n},{provide:pl,useValue:null}])],ngContentSelectors:jF,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(Ee(VF),ie(0),ie(1,1),ce(2,zF,1,0),ce(3,HF,7,0)(4,UF,4,0)),i&2&&(y(2),de(r._isServer?2:-1),y(),de(r._isNativeHtmlTable?3:4))},dependencies:[$_,U_,W_,G_],styles:[`.cdk-table-fixed-layout {
  table-layout: fixed;
}
`],encapsulation:2})}return n})();function lm(n,t){return n.concat(Array.from(t))}function LE(n,t){let e=t.toUpperCase(),i=n.viewContainer.element.nativeElement;for(;i;){let r=i.nodeType===1?i.nodeName:null;if(r===e)return i;if(r==="TABLE")break;i=i.parentNode}return null}var HE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[cl]})}return n})();var GF=[[["caption"]],[["colgroup"],["col"]],"*"],WF=["caption","colgroup, col","*"];function qF(n,t){n&1&&ie(0,2)}function QF(n,t){n&1&&(_(0,"thead",0),nt(1,1),g(),_(2,"tbody",2),nt(3,3)(4,4),g(),_(5,"tfoot",0),nt(6,5),g())}function KF(n,t){n&1&&nt(0,1)(1,3)(2,4)(3,5)}var fm=(()=>{class n extends q_{stickyCssClass="mat-mdc-table-sticky";needsPositionStickyOnElement=!1;static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275cmp=A({type:n,selectors:[["mat-table"],["table","mat-table",""]],hostAttrs:[1,"mat-mdc-table","mdc-data-table__table"],hostVars:2,hostBindings:function(i,r){i&2&&j("mat-table-fixed-layout",r.fixedLayout)},exportAs:["matTable"],features:[ve([{provide:q_,useExisting:n},{provide:Mn,useExisting:n},{provide:pl,useValue:null}]),re],ngContentSelectors:WF,decls:5,vars:2,consts:[["role","rowgroup"],["headerRowOutlet",""],["role","rowgroup",1,"mdc-data-table__content"],["rowOutlet",""],["noDataRowOutlet",""],["footerRowOutlet",""]],template:function(i,r){i&1&&(Ee(GF),ie(0),ie(1,1),ce(2,qF,1,0),ce(3,QF,7,0)(4,KF,4,0)),i&2&&(y(2),de(r._isServer?2:-1),y(),de(r._isNativeHtmlTable?3:4))},dependencies:[$_,U_,W_,G_],styles:[`.mat-mdc-table-sticky {
  position: sticky !important;
}

mat-table {
  display: block;
}

mat-header-row {
  min-height: var(--mat-table-header-container-height, 56px);
}

mat-row {
  min-height: var(--mat-table-row-item-container-height, 52px);
}

mat-footer-row {
  min-height: var(--mat-table-footer-container-height, 52px);
}

mat-row, mat-header-row, mat-footer-row {
  display: flex;
  border-width: 0;
  border-bottom-width: 1px;
  border-style: solid;
  align-items: center;
  box-sizing: border-box;
}

mat-cell:first-of-type, mat-header-cell:first-of-type, mat-footer-cell:first-of-type {
  padding-left: 24px;
}
[dir=rtl] mat-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:first-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:first-of-type:not(:only-of-type) {
  padding-left: 0;
  padding-right: 24px;
}
mat-cell:last-of-type, mat-header-cell:last-of-type, mat-footer-cell:last-of-type {
  padding-right: 24px;
}
[dir=rtl] mat-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-header-cell:last-of-type:not(:only-of-type), [dir=rtl] mat-footer-cell:last-of-type:not(:only-of-type) {
  padding-right: 0;
  padding-left: 24px;
}

mat-cell, mat-header-cell, mat-footer-cell {
  flex: 1;
  display: flex;
  align-items: center;
  overflow: hidden;
  word-wrap: break-word;
  min-height: inherit;
}

.mat-mdc-table {
  min-width: 100%;
  border: 0;
  border-spacing: 0;
  table-layout: auto;
  white-space: normal;
  background-color: var(--mat-table-background-color, var(--mat-sys-surface));
}

.mat-table-fixed-layout {
  table-layout: fixed;
}

.mdc-data-table__cell {
  box-sizing: border-box;
  overflow: hidden;
  text-align: start;
  text-overflow: ellipsis;
}

.mdc-data-table__cell,
.mdc-data-table__header-cell {
  padding: 0 16px;
}

.mat-mdc-header-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-header-container-height, 56px);
  color: var(--mat-table-header-headline-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-header-headline-font, var(--mat-sys-title-small-font, Roboto, sans-serif));
  line-height: var(--mat-table-header-headline-line-height, var(--mat-sys-title-small-line-height));
  font-size: var(--mat-table-header-headline-size, var(--mat-sys-title-small-size, 14px));
  font-weight: var(--mat-table-header-headline-weight, var(--mat-sys-title-small-weight, 500));
}

.mat-mdc-row {
  height: var(--mat-table-row-item-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
}

.mat-mdc-row,
.mdc-data-table__content {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  font-family: var(--mat-table-row-item-label-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-row-item-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-row-item-label-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-row-item-label-text-weight, var(--mat-sys-body-medium-weight));
}

.mat-mdc-footer-row {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  height: var(--mat-table-footer-container-height, 52px);
  color: var(--mat-table-row-item-label-text-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-table-footer-supporting-text-font, var(--mat-sys-body-medium-font, Roboto, sans-serif));
  line-height: var(--mat-table-footer-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-table-footer-supporting-text-size, var(--mat-sys-body-medium-size, 14px));
  font-weight: var(--mat-table-footer-supporting-text-weight, var(--mat-sys-body-medium-weight));
  letter-spacing: var(--mat-table-footer-supporting-text-tracking, var(--mat-sys-body-medium-tracking));
}

.mat-mdc-header-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-header-headline-tracking, var(--mat-sys-title-small-tracking));
  font-weight: inherit;
  line-height: inherit;
  box-sizing: border-box;
  text-overflow: ellipsis;
  overflow: hidden;
  outline: none;
  text-align: start;
}
.mdc-data-table__row:last-child > .mat-mdc-header-cell {
  border-bottom: none;
}

.mat-mdc-cell {
  border-bottom-color: var(--mat-table-row-item-outline-color, var(--mat-sys-outline, rgba(0, 0, 0, 0.12)));
  border-bottom-width: var(--mat-table-row-item-outline-width, 1px);
  border-bottom-style: solid;
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
  line-height: inherit;
}
.mdc-data-table__row:last-child > .mat-mdc-cell {
  border-bottom: none;
}

.mat-mdc-footer-cell {
  letter-spacing: var(--mat-table-row-item-label-text-tracking, var(--mat-sys-body-medium-tracking));
}

mat-row.mat-mdc-row,
mat-header-row.mat-mdc-header-row,
mat-footer-row.mat-mdc-footer-row {
  border-bottom: none;
}

.mat-mdc-table tbody,
.mat-mdc-table tfoot,
.mat-mdc-table thead,
.mat-mdc-cell,
.mat-mdc-footer-cell,
.mat-mdc-header-row,
.mat-mdc-row,
.mat-mdc-footer-row,
.mat-mdc-table .mat-mdc-header-cell {
  background: inherit;
}

.mat-mdc-table mat-header-row.mat-mdc-header-row,
.mat-mdc-table mat-row.mat-mdc-row,
.mat-mdc-table mat-footer-row.mat-mdc-footer-cell {
  height: unset;
}

mat-header-cell.mat-mdc-header-cell,
mat-cell.mat-mdc-cell,
mat-footer-cell.mat-mdc-footer-cell {
  align-self: stretch;
}
`],encapsulation:2})}return n})(),pm=(()=>{class n extends dm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["","matCellDef",""]],features:[ve([{provide:dm,useExisting:n}]),re]})}return n})(),gm=(()=>{class n extends um{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["","matHeaderCellDef",""]],features:[ve([{provide:um,useExisting:n}]),re]})}return n})();var _m=(()=>{class n extends _s{get name(){return this._name}set name(e){this._setNameInput(e)}_updateColumnCssClassName(){super._updateColumnCssClassName(),this._columnCssClassName.push(`mat-column-${this.cssClassFriendlyName}`)}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["","matColumnDef",""]],inputs:{name:[0,"matColumnDef","name"]},features:[ve([{provide:_s,useExisting:n}]),re]})}return n})(),vm=(()=>{class n extends VE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["mat-header-cell"],["th","mat-header-cell",""]],hostAttrs:["role","columnheader",1,"mat-mdc-header-cell","mdc-data-table__header-cell"],features:[re]})}return n})();var bm=(()=>{class n extends jE{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["mat-cell"],["td","mat-cell",""]],hostAttrs:[1,"mat-mdc-cell","mdc-data-table__cell"],features:[re]})}return n})();var ym=(()=>{class n extends gl{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["","matHeaderRowDef",""]],inputs:{columns:[0,"matHeaderRowDef","columns"],sticky:[2,"matHeaderRowDefSticky","sticky",N]},features:[ve([{provide:gl,useExisting:n}]),re]})}return n})();var wm=(()=>{class n extends mm{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275dir=T({type:n,selectors:[["","matRowDef",""]],inputs:{columns:[0,"matRowDefColumns","columns"],when:[0,"matRowDefWhen","when"]},features:[ve([{provide:mm,useExisting:n}]),re]})}return n})(),Cm=(()=>{class n extends z_{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275cmp=A({type:n,selectors:[["mat-header-row"],["tr","mat-header-row",""]],hostAttrs:["role","row",1,"mat-mdc-header-row","mdc-data-table__header-row"],exportAs:["matHeaderRow"],features:[ve([{provide:z_,useExisting:n}]),re],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&nt(0,0)},dependencies:[eo],encapsulation:2})}return n})();var Dm=(()=>{class n extends H_{static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275cmp=A({type:n,selectors:[["mat-row"],["tr","mat-row",""]],hostAttrs:["role","row",1,"mat-mdc-row","mdc-data-table__row"],exportAs:["matRow"],features:[ve([{provide:H_,useExisting:n}]),re],decls:1,vars:0,consts:[["cdkCellOutlet",""]],template:function(i,r){i&1&&nt(0,0)},dependencies:[eo],encapsulation:2})}return n})();var UE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[HE,oe]})}return n})(),YF=9007199254740991,hm=class extends al{_data;_renderData=new Bt([]);_filter=new Bt("");_internalPageChanges=new k;_renderChangesSubscription=null;filteredData;get data(){return this._data.value}set data(t){t=Array.isArray(t)?t:[],this._data.next(t),this._renderChangesSubscription||this._filterData(t)}get filter(){return this._filter.value}set filter(t){this._filter.next(t),this._renderChangesSubscription||this._filterData(this.data)}get sort(){return this._sort}set sort(t){this._sort=t,this._updateChangeSubscription()}_sort;get paginator(){return this._paginator}set paginator(t){this._paginator=t,this._updateChangeSubscription()}_paginator;sortingDataAccessor=(t,e)=>{let i=t[e];if(Nu(i)){let r=Number(i);return r<YF?r:i}return i};sortData=(t,e)=>{let i=e.active,r=e.direction;return!i||r==""?t:t.sort((o,s)=>{let a=this.sortingDataAccessor(o,i),l=this.sortingDataAccessor(s,i),c=typeof a,d=typeof l;c!==d&&(c==="number"&&(a+=""),d==="number"&&(l+=""));let m=0;return a!=null&&l!=null?a>l?m=1:a<l&&(m=-1):a!=null?m=1:l!=null&&(m=-1),m*(r=="asc"?1:-1)})};filterPredicate=(t,e)=>{let i=e.trim().toLowerCase();return Object.values(t).some(r=>`${r}`.toLowerCase().includes(i))};constructor(t=[]){super(),this._data=new Bt(t),this._updateChangeSubscription()}_updateChangeSubscription(){let t=this._sort?Zt(this._sort.sortChange,this._sort.initialized):$e(null),e=this._paginator?Zt(this._paginator.page,this._internalPageChanges,this._paginator.initialized):$e(null),i=this._data,r=hn([i,this._filter]).pipe(ye(([a])=>this._filterData(a))),o=hn([r,t]).pipe(ye(([a])=>this._orderData(a))),s=hn([o,e]).pipe(ye(([a])=>this._pageData(a)));this._renderChangesSubscription?.unsubscribe(),this._renderChangesSubscription=s.subscribe(a=>this._renderData.next(a))}_filterData(t){return this.filteredData=this.filter==null||this.filter===""?t:t.filter(e=>this.filterPredicate(e,this.filter)),this.paginator&&this._updatePaginator(this.filteredData.length),this.filteredData}_orderData(t){return this.sort?this.sortData(t.slice(),this.sort):t}_pageData(t){if(!this.paginator)return t;let e=this.paginator.pageIndex*this.paginator.pageSize;return t.slice(e,e+this.paginator.pageSize)}_updatePaginator(t){Promise.resolve().then(()=>{let e=this.paginator;if(e&&(e.length=t,e.pageIndex>0)){let i=Math.ceil(e.length/e.pageSize)-1||0,r=Math.min(e.pageIndex,i);r!==e.pageIndex&&(e.pageIndex=r,this._internalPageChanges.next())}})}connect(){return this._renderChangesSubscription||this._updateChangeSubscription(),this._renderData}disconnect(){this._renderChangesSubscription?.unsubscribe(),this._renderChangesSubscription=null}};function ZF(n,t){}var nr=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;positionStrategy;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;scrollStrategy;closeOnNavigation=!0;closeOnDestroy=!0;closeOnOverlayDetachments=!0;disableAnimations=!1;providers;container;templateContext};var K_=(()=>{class n extends Zi{_elementRef=u(P);_focusTrapFactory=u(l_);_config;_interactivityChecker=u(Bu);_ngZone=u(F);_focusMonitor=u(mi);_renderer=u(Ke);_changeDetectorRef=u(Ae);_injector=u($);_platform=u(_e);_document=u(U);_portalOutlet;_focusTrapped=new k;_focusTrap=null;_elementFocusedBeforeDialogWasOpened=null;_closeInteractionType=null;_ariaLabelledByQueue=[];_isDestroyed=!1;constructor(){super(),this._config=u(nr,{optional:!0})||new nr,this._config.ariaLabelledBy&&this._ariaLabelledByQueue.push(this._config.ariaLabelledBy)}_addAriaLabelledBy(e){this._ariaLabelledByQueue.push(e),this._changeDetectorRef.markForCheck()}_removeAriaLabelledBy(e){let i=this._ariaLabelledByQueue.indexOf(e);i>-1&&(this._ariaLabelledByQueue.splice(i,1),this._changeDetectorRef.markForCheck())}_contentAttached(){this._initializeFocusTrap(),this._captureInitialFocus()}_captureInitialFocus(){this._trapFocus()}ngOnDestroy(){this._focusTrapped.complete(),this._isDestroyed=!0,this._restoreFocus()}attachComponentPortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._contentAttached(),i}attachTemplatePortal(e){this._portalOutlet.hasAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._contentAttached(),i}attachDomPortal=e=>{this._portalOutlet.hasAttached();let i=this._portalOutlet.attachDomPortal(e);return this._contentAttached(),i};_recaptureFocus(){this._containsFocus()||this._trapFocus()}_forceFocus(e,i){this._interactivityChecker.isFocusable(e)||(e.tabIndex=-1,this._ngZone.runOutsideAngular(()=>{let r=()=>{o(),s(),e.removeAttribute("tabindex")},o=this._renderer.listen(e,"blur",r),s=this._renderer.listen(e,"mousedown",r)})),e.focus(i)}_focusByCssSelector(e,i){let r=this._elementRef.nativeElement.querySelector(e);r&&this._forceFocus(r,i)}_trapFocus(e){this._isDestroyed||Qe(()=>{let i=this._elementRef.nativeElement;switch(this._config.autoFocus){case!1:case"dialog":this._containsFocus()||i.focus(e);break;case!0:case"first-tabbable":this._focusTrap?.focusInitialElement(e)||this._focusDialogContainer(e);break;case"first-heading":this._focusByCssSelector('h1, h2, h3, h4, h5, h6, [role="heading"]',e);break;default:this._focusByCssSelector(this._config.autoFocus,e);break}this._focusTrapped.next()},{injector:this._injector})}_restoreFocus(){let e=this._config.restoreFocus,i=null;if(typeof e=="string"?i=this._document.querySelector(e):typeof e=="boolean"?i=e?this._elementFocusedBeforeDialogWasOpened:null:e&&(i=e),this._config.restoreFocus&&i&&typeof i.focus=="function"){let r=Ka(),o=this._elementRef.nativeElement;(!r||r===this._document.body||r===o||o.contains(r))&&(this._focusMonitor?(this._focusMonitor.focusVia(i,this._closeInteractionType),this._closeInteractionType=null):i.focus())}this._focusTrap&&this._focusTrap.destroy()}_focusDialogContainer(e){this._elementRef.nativeElement.focus?.(e)}_containsFocus(){let e=this._elementRef.nativeElement,i=Ka();return e===i||e.contains(i)}_initializeFocusTrap(){this._platform.isBrowser&&(this._focusTrap=this._focusTrapFactory.create(this._elementRef.nativeElement),this._document&&(this._elementFocusedBeforeDialogWasOpened=Ka()))}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["cdk-dialog-container"]],viewQuery:function(i,r){if(i&1&&Te(Xi,7),i&2){let o;G(o=W())&&(r._portalOutlet=o.first)}},hostAttrs:["tabindex","-1",1,"cdk-dialog-container"],hostVars:6,hostBindings:function(i,r){i&2&&J("id",r._config.id||null)("role",r._config.role)("aria-modal",r._config.ariaModal)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null)},features:[re],decls:1,vars:0,consts:[["cdkPortalOutlet",""]],template:function(i,r){i&1&&fe(0,ZF,0,0,"ng-template",0)},dependencies:[Xi],styles:[`.cdk-dialog-container {
  display: block;
  width: 100%;
  height: 100%;
  min-height: inherit;
  max-height: inherit;
}
`],encapsulation:2})}return n})(),_l=class{overlayRef;config;componentInstance=null;componentRef=null;containerInstance;disableClose;closed=new k;backdropClick;keydownEvents;outsidePointerEvents;id;_detachSubscription;constructor(t,e){this.overlayRef=t,this.config=e,this.disableClose=e.disableClose,this.backdropClick=t.backdropClick(),this.keydownEvents=t.keydownEvents(),this.outsidePointerEvents=t.outsidePointerEvents(),this.id=e.id,this.keydownEvents.subscribe(i=>{i.keyCode===27&&!this.disableClose&&!ut(i)&&(i.preventDefault(),this.close(void 0,{focusOrigin:"keyboard"}))}),this.backdropClick.subscribe(()=>{!this.disableClose&&this._canClose()?this.close(void 0,{focusOrigin:"mouse"}):this.containerInstance._recaptureFocus?.()}),this._detachSubscription=t.detachments().subscribe(()=>{e.closeOnOverlayDetachments!==!1&&this.close()})}close(t,e){if(this._canClose(t)){let i=this.closed;this.containerInstance._closeInteractionType=e?.focusOrigin||"program",this._detachSubscription.unsubscribe(),this.overlayRef.dispose(),i.next(t),i.complete(),this.componentInstance=this.containerInstance=null}}updatePosition(){return this.overlayRef.updatePosition(),this}updateSize(t="",e=""){return this.overlayRef.updateSize({width:t,height:e}),this}addPanelClass(t){return this.overlayRef.addPanelClass(t),this}removePanelClass(t){return this.overlayRef.removePanelClass(t),this}_canClose(t){let e=this.config;return!!this.containerInstance&&(!e.closePredicate||e.closePredicate(t,e,this.componentInstance))}},XF=new v("DialogScrollStrategy",{providedIn:"root",factory:()=>{let n=u($);return()=>fs(n)}}),JF=new v("DialogData"),eP=new v("DefaultDialogConfig");function tP(n){let t=Oe(n),e=new z;return{valueSignal:t,get value(){return t()},change:e,ngOnDestroy(){e.complete()}}}var Y_=(()=>{class n{_injector=u($);_defaultOptions=u(eP,{optional:!0});_parentDialog=u(n,{optional:!0,skipSelf:!0});_overlayContainer=u(rm);_idGenerator=u(Ie);_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new k;_afterOpenedAtThisLevel=new k;_ariaHiddenElements=new Map;_scrollStrategy=u(XF);get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}afterAllClosed=hr(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(jt(void 0)));constructor(){}open(e,i){let r=this._defaultOptions||new nr;i=M(M({},r),i),i.id=i.id||this._idGenerator.getId("cdk-dialog-"),i.id&&this.getDialogById(i.id);let o=this._getOverlayConfig(i),s=vi(this._injector,o),a=new _l(s,i),l=this._attachContainer(s,a,i);if(a.containerInstance=l,!this.openDialogs.length){let c=this._overlayContainer.getContainerElement();l._focusTrapped?l._focusTrapped.pipe(Vt(1)).subscribe(()=>{this._hideNonDialogContentFromAssistiveTechnology(c)}):this._hideNonDialogContentFromAssistiveTechnology(c)}return this._attachDialogContent(e,a,l,i),this.openDialogs.push(a),a.closed.subscribe(()=>this._removeOpenDialog(a,!0)),this.afterOpened.next(a),a}closeAll(){Q_(this.openDialogs,e=>e.close())}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){Q_(this._openDialogsAtThisLevel,e=>{e.config.closeOnDestroy===!1&&this._removeOpenDialog(e,!1)}),Q_(this._openDialogsAtThisLevel,e=>e.close()),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete(),this._openDialogsAtThisLevel=[]}_getOverlayConfig(e){let i=new _i({positionStrategy:e.positionStrategy||er().centerHorizontally().centerVertically(),scrollStrategy:e.scrollStrategy||this._scrollStrategy(),panelClass:e.panelClass,hasBackdrop:e.hasBackdrop,direction:e.direction,minWidth:e.minWidth,minHeight:e.minHeight,maxWidth:e.maxWidth,maxHeight:e.maxHeight,width:e.width,height:e.height,disposeOnNavigation:e.closeOnNavigation,disableAnimations:e.disableAnimations});return e.backdropClass&&(i.backdropClass=e.backdropClass),i}_attachContainer(e,i,r){let o=r.injector||r.viewContainerRef?.injector,s=[{provide:nr,useValue:r},{provide:_l,useValue:i},{provide:ms,useValue:e}],a;r.container?typeof r.container=="function"?a=r.container:(a=r.container.type,s.push(...r.container.providers(r))):a=K_;let l=new In(a,r.viewContainerRef,$.create({parent:o||this._injector,providers:s}));return e.attach(l).instance}_attachDialogContent(e,i,r,o){if(e instanceof Pe){let s=this._createInjector(o,i,r,void 0),a={$implicit:o.data,dialogRef:i};o.templateContext&&(a=M(M({},a),typeof o.templateContext=="function"?o.templateContext():o.templateContext)),r.attachTemplatePortal(new gi(e,null,a,s))}else{let s=this._createInjector(o,i,r,this._injector),a=r.attachComponentPortal(new In(e,o.viewContainerRef,s));i.componentRef=a,i.componentInstance=a.instance}}_createInjector(e,i,r,o){let s=e.injector||e.viewContainerRef?.injector,a=[{provide:JF,useValue:e.data},{provide:_l,useValue:i}];return e.providers&&(typeof e.providers=="function"?a.push(...e.providers(i,e,r)):a.push(...e.providers)),e.direction&&(!s||!s.get(st,null,{optional:!0}))&&a.push({provide:st,useValue:tP(e.direction)}),$.create({parent:s||o,providers:a})}_removeOpenDialog(e,i){let r=this.openDialogs.indexOf(e);r>-1&&(this.openDialogs.splice(r,1),this.openDialogs.length||(this._ariaHiddenElements.forEach((o,s)=>{o?s.setAttribute("aria-hidden",o):s.removeAttribute("aria-hidden")}),this._ariaHiddenElements.clear(),i&&this._getAfterAllClosed().next()))}_hideNonDialogContentFromAssistiveTechnology(e){if(e.parentElement){let i=e.parentElement.children;for(let r=i.length-1;r>-1;r--){let o=i[r];o!==e&&o.nodeName!=="SCRIPT"&&o.nodeName!=="STYLE"&&!o.hasAttribute("aria-live")&&!o.hasAttribute("popover")&&(this._ariaHiddenElements.set(o,o.getAttribute("aria-hidden")),o.setAttribute("aria-hidden","true"))}}}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function Q_(n,t){let e=n.length;for(;e--;)t(n[e])}var GE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[Y_],imports:[Qn,Ji,qr,Ji]})}return n})();function nP(n,t){}var Em=class{viewContainerRef;injector;id;role="dialog";panelClass="";hasBackdrop=!0;backdropClass="";disableClose=!1;closePredicate;width="";height="";minWidth;minHeight;maxWidth;maxHeight;position;data=null;direction;ariaDescribedBy=null;ariaLabelledBy=null;ariaLabel=null;ariaModal=!1;autoFocus="first-tabbable";restoreFocus=!0;delayFocusTrap=!0;scrollStrategy;closeOnNavigation=!0;enterAnimationDuration;exitAnimationDuration},Z_="mdc-dialog--open",WE="mdc-dialog--opening",qE="mdc-dialog--closing",iP=150,rP=75,oP=(()=>{class n extends K_{_animationStateChanged=new z;_animationsEnabled=!Fe();_actionSectionCount=0;_hostElement=this._elementRef.nativeElement;_enterAnimationDuration=this._animationsEnabled?KE(this._config.enterAnimationDuration)??iP:0;_exitAnimationDuration=this._animationsEnabled?KE(this._config.exitAnimationDuration)??rP:0;_animationTimer=null;_contentAttached(){super._contentAttached(),this._startOpenAnimation()}_startOpenAnimation(){this._animationStateChanged.emit({state:"opening",totalTime:this._enterAnimationDuration}),this._animationsEnabled?(this._hostElement.style.setProperty(QE,`${this._enterAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(WE,Z_)),this._waitForAnimationToComplete(this._enterAnimationDuration,this._finishDialogOpen)):(this._hostElement.classList.add(Z_),Promise.resolve().then(()=>this._finishDialogOpen()))}_startExitAnimation(){this._animationStateChanged.emit({state:"closing",totalTime:this._exitAnimationDuration}),this._hostElement.classList.remove(Z_),this._animationsEnabled?(this._hostElement.style.setProperty(QE,`${this._exitAnimationDuration}ms`),this._requestAnimationFrame(()=>this._hostElement.classList.add(qE)),this._waitForAnimationToComplete(this._exitAnimationDuration,this._finishDialogClose)):Promise.resolve().then(()=>this._finishDialogClose())}_updateActionSectionCount(e){this._actionSectionCount+=e,this._changeDetectorRef.markForCheck()}_finishDialogOpen=()=>{this._clearAnimationClasses(),this._openAnimationDone(this._enterAnimationDuration)};_finishDialogClose=()=>{this._clearAnimationClasses(),this._animationStateChanged.emit({state:"closed",totalTime:this._exitAnimationDuration})};_clearAnimationClasses(){this._hostElement.classList.remove(WE,qE)}_waitForAnimationToComplete(e,i){this._animationTimer!==null&&clearTimeout(this._animationTimer),this._animationTimer=setTimeout(i,e)}_requestAnimationFrame(e){this._ngZone.runOutsideAngular(()=>{typeof requestAnimationFrame=="function"?requestAnimationFrame(e):e()})}_captureInitialFocus(){this._config.delayFocusTrap||this._trapFocus()}_openAnimationDone(e){this._config.delayFocusTrap&&this._trapFocus(),this._animationStateChanged.next({state:"opened",totalTime:e})}ngOnDestroy(){super.ngOnDestroy(),this._animationTimer!==null&&clearTimeout(this._animationTimer)}attachComponentPortal(e){let i=super.attachComponentPortal(e);return i.location.nativeElement.classList.add("mat-mdc-dialog-component-host"),i}static \u0275fac=(()=>{let e;return function(r){return(e||(e=Be(n)))(r||n)}})();static \u0275cmp=A({type:n,selectors:[["mat-dialog-container"]],hostAttrs:["tabindex","-1",1,"mat-mdc-dialog-container","mdc-dialog"],hostVars:10,hostBindings:function(i,r){i&2&&(Nt("id",r._config.id),J("aria-modal",r._config.ariaModal)("role",r._config.role)("aria-labelledby",r._config.ariaLabel?null:r._ariaLabelledByQueue[0])("aria-label",r._config.ariaLabel)("aria-describedby",r._config.ariaDescribedBy||null),j("_mat-animation-noopable",!r._animationsEnabled)("mat-mdc-dialog-container-with-actions",r._actionSectionCount>0))},features:[re],decls:3,vars:0,consts:[[1,"mat-mdc-dialog-inner-container","mdc-dialog__container"],[1,"mat-mdc-dialog-surface","mdc-dialog__surface"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(_(0,"div",0)(1,"div",1),fe(2,nP,0,0,"ng-template",2),g()())},dependencies:[Xi],styles:[`.mat-mdc-dialog-container {
  width: 100%;
  height: 100%;
  display: block;
  box-sizing: border-box;
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  outline: 0;
}

.cdk-overlay-pane.mat-mdc-dialog-panel {
  max-width: var(--mat-dialog-container-max-width, 560px);
  min-width: var(--mat-dialog-container-min-width, 280px);
}
@media (max-width: 599px) {
  .cdk-overlay-pane.mat-mdc-dialog-panel {
    max-width: var(--mat-dialog-container-small-max-width, calc(100vw - 32px));
  }
}

.mat-mdc-dialog-inner-container {
  display: flex;
  flex-direction: row;
  align-items: center;
  justify-content: space-around;
  box-sizing: border-box;
  height: 100%;
  opacity: 0;
  transition: opacity linear var(--mat-dialog-transition-duration, 0ms);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
}
.mdc-dialog--closing .mat-mdc-dialog-inner-container {
  transition: opacity 75ms linear;
  transform: none;
}
.mdc-dialog--open .mat-mdc-dialog-inner-container {
  opacity: 1;
}
._mat-animation-noopable .mat-mdc-dialog-inner-container {
  transition: none;
}

.mat-mdc-dialog-surface {
  display: flex;
  flex-direction: column;
  flex-grow: 0;
  flex-shrink: 0;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  position: relative;
  overflow-y: auto;
  outline: 0;
  transform: scale(0.8);
  transition: transform var(--mat-dialog-transition-duration, 0ms) cubic-bezier(0, 0, 0.2, 1);
  max-height: inherit;
  min-height: inherit;
  min-width: inherit;
  max-width: inherit;
  box-shadow: var(--mat-dialog-container-elevation-shadow, none);
  border-radius: var(--mat-dialog-container-shape, var(--mat-sys-corner-extra-large, 4px));
  background-color: var(--mat-dialog-container-color, var(--mat-sys-surface, white));
}
[dir=rtl] .mat-mdc-dialog-surface {
  text-align: right;
}
.mdc-dialog--open .mat-mdc-dialog-surface, .mdc-dialog--closing .mat-mdc-dialog-surface {
  transform: none;
}
._mat-animation-noopable .mat-mdc-dialog-surface {
  transition: none;
}
.mat-mdc-dialog-surface::before {
  position: absolute;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  top: 0;
  left: 0;
  border: 2px solid transparent;
  border-radius: inherit;
  content: "";
  pointer-events: none;
}

.mat-mdc-dialog-title {
  display: block;
  position: relative;
  flex-shrink: 0;
  box-sizing: border-box;
  margin: 0 0 1px;
  padding: var(--mat-dialog-headline-padding, 6px 24px 13px);
}
.mat-mdc-dialog-title::before {
  display: inline-block;
  width: 0;
  height: 40px;
  content: "";
  vertical-align: 0;
}
[dir=rtl] .mat-mdc-dialog-title {
  text-align: right;
}
.mat-mdc-dialog-container .mat-mdc-dialog-title {
  color: var(--mat-dialog-subhead-color, var(--mat-sys-on-surface, rgba(0, 0, 0, 0.87)));
  font-family: var(--mat-dialog-subhead-font, var(--mat-sys-headline-small-font, inherit));
  line-height: var(--mat-dialog-subhead-line-height, var(--mat-sys-headline-small-line-height, 1.5rem));
  font-size: var(--mat-dialog-subhead-size, var(--mat-sys-headline-small-size, 1rem));
  font-weight: var(--mat-dialog-subhead-weight, var(--mat-sys-headline-small-weight, 400));
  letter-spacing: var(--mat-dialog-subhead-tracking, var(--mat-sys-headline-small-tracking, 0.03125em));
}

.mat-mdc-dialog-content {
  display: block;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  overflow: auto;
  max-height: 65vh;
}
.mat-mdc-dialog-content > :first-child {
  margin-top: 0;
}
.mat-mdc-dialog-content > :last-child {
  margin-bottom: 0;
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  color: var(--mat-dialog-supporting-text-color, var(--mat-sys-on-surface-variant, rgba(0, 0, 0, 0.6)));
  font-family: var(--mat-dialog-supporting-text-font, var(--mat-sys-body-medium-font, inherit));
  line-height: var(--mat-dialog-supporting-text-line-height, var(--mat-sys-body-medium-line-height, 1.5rem));
  font-size: var(--mat-dialog-supporting-text-size, var(--mat-sys-body-medium-size, 1rem));
  font-weight: var(--mat-dialog-supporting-text-weight, var(--mat-sys-body-medium-weight, 400));
  letter-spacing: var(--mat-dialog-supporting-text-tracking, var(--mat-sys-body-medium-tracking, 0.03125em));
}
.mat-mdc-dialog-container .mat-mdc-dialog-content {
  padding: var(--mat-dialog-content-padding, 20px 24px);
}
.mat-mdc-dialog-container-with-actions .mat-mdc-dialog-content {
  padding: var(--mat-dialog-with-actions-content-padding, 20px 24px 0);
}
.mat-mdc-dialog-container .mat-mdc-dialog-title + .mat-mdc-dialog-content {
  padding-top: 0;
}

.mat-mdc-dialog-actions {
  display: flex;
  position: relative;
  flex-shrink: 0;
  flex-wrap: wrap;
  align-items: center;
  box-sizing: border-box;
  min-height: 52px;
  margin: 0;
  border-top: 1px solid transparent;
  padding: var(--mat-dialog-actions-padding, 16px 24px);
  justify-content: var(--mat-dialog-actions-alignment, flex-end);
}
@media (forced-colors: active) {
  .mat-mdc-dialog-actions {
    border-top-color: CanvasText;
  }
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-start, .mat-mdc-dialog-actions[align=start] {
  justify-content: start;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-center, .mat-mdc-dialog-actions[align=center] {
  justify-content: center;
}
.mat-mdc-dialog-actions.mat-mdc-dialog-actions-align-end, .mat-mdc-dialog-actions[align=end] {
  justify-content: flex-end;
}
.mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
.mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 8px;
}
[dir=rtl] .mat-mdc-dialog-actions .mat-button-base + .mat-button-base,
[dir=rtl] .mat-mdc-dialog-actions .mat-mdc-button-base + .mat-mdc-button-base {
  margin-left: 0;
  margin-right: 8px;
}

.mat-mdc-dialog-component-host {
  display: contents;
}
`],encapsulation:2})}return n})(),QE="--mat-dialog-transition-duration";function KE(n){return n==null?null:typeof n=="number"?n:n.endsWith("ms")?ui(n.substring(0,n.length-2)):n.endsWith("s")?ui(n.substring(0,n.length-1))*1e3:n==="0"?0:null}var xm=(function(n){return n[n.OPEN=0]="OPEN",n[n.CLOSING=1]="CLOSING",n[n.CLOSED=2]="CLOSED",n})(xm||{}),vl=class{_ref;_config;_containerInstance;componentInstance;componentRef=null;disableClose;id;_afterOpened=new Kt(1);_beforeClosed=new Kt(1);_result;_closeFallbackTimeout;_state=xm.OPEN;_closeInteractionType;constructor(t,e,i){this._ref=t,this._config=e,this._containerInstance=i,this.disableClose=e.disableClose,this.id=t.id,t.addPanelClass("mat-mdc-dialog-panel"),i._animationStateChanged.pipe(ze(r=>r.state==="opened"),Vt(1)).subscribe(()=>{this._afterOpened.next(),this._afterOpened.complete()}),i._animationStateChanged.pipe(ze(r=>r.state==="closed"),Vt(1)).subscribe(()=>{clearTimeout(this._closeFallbackTimeout),this._finishDialogClose()}),t.overlayRef.detachments().subscribe(()=>{this._beforeClosed.next(this._result),this._beforeClosed.complete(),this._finishDialogClose()}),Zt(this.backdropClick(),this.keydownEvents().pipe(ze(r=>r.keyCode===27&&!this.disableClose&&!ut(r)))).subscribe(r=>{this.disableClose||(r.preventDefault(),sP(this,r.type==="keydown"?"keyboard":"mouse"))})}close(t){let e=this._config.closePredicate;e&&!e(t,this._config,this.componentInstance)||(this._result=t,this._containerInstance._animationStateChanged.pipe(ze(i=>i.state==="closing"),Vt(1)).subscribe(i=>{this._beforeClosed.next(t),this._beforeClosed.complete(),this._ref.overlayRef.detachBackdrop(),this._closeFallbackTimeout=setTimeout(()=>this._finishDialogClose(),i.totalTime+100)}),this._state=xm.CLOSING,this._containerInstance._startExitAnimation())}afterOpened(){return this._afterOpened}afterClosed(){return this._ref.closed}beforeClosed(){return this._beforeClosed}backdropClick(){return this._ref.backdropClick}keydownEvents(){return this._ref.keydownEvents}updatePosition(t){let e=this._ref.config.positionStrategy;return t&&(t.left||t.right)?t.left?e.left(t.left):e.right(t.right):e.centerHorizontally(),t&&(t.top||t.bottom)?t.top?e.top(t.top):e.bottom(t.bottom):e.centerVertically(),this._ref.updatePosition(),this}updateSize(t="",e=""){return this._ref.updateSize(t,e),this}addPanelClass(t){return this._ref.addPanelClass(t),this}removePanelClass(t){return this._ref.removePanelClass(t),this}getState(){return this._state}_finishDialogClose(){this._state=xm.CLOSED,this._ref.close(this._result,{focusOrigin:this._closeInteractionType}),this.componentInstance=null}};function sP(n,t,e){return n._closeInteractionType=t,n.close(e)}var X_=new v("MatMdcDialogData"),aP=new v("mat-mdc-dialog-default-options"),lP=new v("mat-mdc-dialog-scroll-strategy",{providedIn:"root",factory:()=>{let n=u($);return()=>fs(n)}}),J_=(()=>{class n{_defaultOptions=u(aP,{optional:!0});_scrollStrategy=u(lP);_parentDialog=u(n,{optional:!0,skipSelf:!0});_idGenerator=u(Ie);_injector=u($);_dialog=u(Y_);_animationsDisabled=Fe();_openDialogsAtThisLevel=[];_afterAllClosedAtThisLevel=new k;_afterOpenedAtThisLevel=new k;dialogConfigClass=Em;_dialogRefConstructor;_dialogContainerType;_dialogDataToken;get openDialogs(){return this._parentDialog?this._parentDialog.openDialogs:this._openDialogsAtThisLevel}get afterOpened(){return this._parentDialog?this._parentDialog.afterOpened:this._afterOpenedAtThisLevel}_getAfterAllClosed(){let e=this._parentDialog;return e?e._getAfterAllClosed():this._afterAllClosedAtThisLevel}afterAllClosed=hr(()=>this.openDialogs.length?this._getAfterAllClosed():this._getAfterAllClosed().pipe(jt(void 0)));constructor(){this._dialogRefConstructor=vl,this._dialogContainerType=oP,this._dialogDataToken=X_}open(e,i){let r;i=M(M({},this._defaultOptions||new Em),i),i.id=i.id||this._idGenerator.getId("mat-mdc-dialog-"),i.scrollStrategy=i.scrollStrategy||this._scrollStrategy();let o=this._dialog.open(e,be(M({},i),{positionStrategy:er(this._injector).centerHorizontally().centerVertically(),disableClose:!0,closePredicate:void 0,closeOnDestroy:!1,closeOnOverlayDetachments:!1,disableAnimations:this._animationsDisabled||i.enterAnimationDuration?.toLocaleString()==="0"||i.exitAnimationDuration?.toString()==="0",container:{type:this._dialogContainerType,providers:()=>[{provide:this.dialogConfigClass,useValue:i},{provide:nr,useValue:i}]},templateContext:()=>({dialogRef:r}),providers:(s,a,l)=>(r=new this._dialogRefConstructor(s,i,l),r.updatePosition(i?.position),[{provide:this._dialogContainerType,useValue:l},{provide:this._dialogDataToken,useValue:a.data},{provide:this._dialogRefConstructor,useValue:r}])}));return r.componentRef=o.componentRef,r.componentInstance=o.componentInstance,this.openDialogs.push(r),this.afterOpened.next(r),r.afterClosed().subscribe(()=>{let s=this.openDialogs.indexOf(r);s>-1&&(this.openDialogs.splice(s,1),this.openDialogs.length||this._getAfterAllClosed().next())}),r}closeAll(){this._closeDialogs(this.openDialogs)}getDialogById(e){return this.openDialogs.find(i=>i.id===e)}ngOnDestroy(){this._closeDialogs(this._openDialogsAtThisLevel),this._afterAllClosedAtThisLevel.complete(),this._afterOpenedAtThisLevel.complete()}_closeDialogs(e){let i=e.length;for(;i--;)e[i].close()}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();var YE=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[J_],imports:[GE,Qn,Ji,oe]})}return n})();var XE=(()=>{class n{constructor(e,i){this.dialogRef=e,this.data=i}onOkClick(){this.dialogRef.close(this.comment)}onNoClick(){this.dialogRef.close(!1)}ngOnInit(){this.comment=this.data}static{this.\u0275fac=function(i){return new(i||n)(q(vl),q(X_))}}static{this.\u0275cmp=A({type:n,selectors:[["app-comment-dialog"]],standalone:!1,decls:12,vars:1,consts:[[1,"main-dialog-div"],["appearance","outline","id","productNameField"],[1,"mat-label"],["matInput","","autocomplete","off",3,"ngModelChange","ngModel"],[1,"buttons-container"],["mat-raised-button","","color","accent","id","dialog-button-save",3,"click"],[1,"button-text"],["mat-raised-button","","color","primary","id","dialog-button-close",3,"click"]],template:function(i,r){i&1&&(_(0,"div",0)(1,"mat-form-field",1)(2,"mat-label",2),E(3,"\u041A\u043E\u043C\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439"),g(),_(4,"input",3),Nd("ngModelChange",function(s){return jp(r.comment,s)||(r.comment=s),s}),g()(),_(5,"div",4)(6,"button",5),V("click",function(){return r.onOkClick()}),_(7,"span",6),E(8,"\u041E\u043A"),g()(),_(9,"button",7),V("click",function(){return r.onNoClick()}),_(10,"span",6),E(11,"\u041E\u0442\u043C\u0435\u043D\u0430"),g()()()()),i&2&&(y(4),Od("ngModel",r.comment))},dependencies:[es,Tu,e_,Gn,Wn,pi,us],styles:[".main-dialog-div[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:stretch;justify-content:center;width:100%;box-sizing:border-box;padding:16px}mat-form-field[_ngcontent-%COMP%]{width:100%;box-sizing:border-box}.buttons-container[_ngcontent-%COMP%]{display:flex;justify-content:center;width:100%;gap:8px;box-sizing:border-box}.buttons-container[_ngcontent-%COMP%]   button[_ngcontent-%COMP%]{margin:0;padding:0;width:42%}"]})}}return n})();function cP(n,t){if(n&1){let e=Je();_(0,"div",1)(1,"button",2),V("click",function(){He(e);let r=H();return Ue(r.action())}),E(2),g()()}if(n&2){let e=H();y(2),Ye(" ",e.data.action," ")}}var dP=["label"];function uP(n,t){}var mP=Math.pow(2,31)-1,bl=class{_overlayRef;instance;containerInstance;_afterDismissed=new k;_afterOpened=new k;_onAction=new k;_durationTimeoutId;_dismissedByAction=!1;constructor(t,e){this._overlayRef=e,this.containerInstance=t,t._onExit.subscribe(()=>this._finishDismiss())}dismiss(){this._afterDismissed.closed||this.containerInstance.exit(),clearTimeout(this._durationTimeoutId)}dismissWithAction(){this._onAction.closed||(this._dismissedByAction=!0,this._onAction.next(),this._onAction.complete(),this.dismiss()),clearTimeout(this._durationTimeoutId)}closeWithAction(){this.dismissWithAction()}_dismissAfter(t){this._durationTimeoutId=setTimeout(()=>this.dismiss(),Math.min(t,mP))}_open(){this._afterOpened.closed||(this._afterOpened.next(),this._afterOpened.complete())}_finishDismiss(){this._overlayRef.dispose(),this._onAction.closed||this._onAction.complete(),this._afterDismissed.next({dismissedByAction:this._dismissedByAction}),this._afterDismissed.complete(),this._dismissedByAction=!1}afterDismissed(){return this._afterDismissed}afterOpened(){return this.containerInstance._onEnter}onAction(){return this._onAction}},JE=new v("MatSnackBarData"),vs=class{politeness="polite";announcementMessage="";viewContainerRef;duration=0;panelClass;direction;data=null;horizontalPosition="center";verticalPosition="bottom"},hP=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","matSnackBarLabel",""]],hostAttrs:[1,"mat-mdc-snack-bar-label","mdc-snackbar__label"]})}return n})(),fP=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","matSnackBarActions",""]],hostAttrs:[1,"mat-mdc-snack-bar-actions","mdc-snackbar__actions"]})}return n})(),pP=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["","matSnackBarAction",""]],hostAttrs:[1,"mat-mdc-snack-bar-action","mdc-snackbar__action"]})}return n})(),gP=(()=>{class n{snackBarRef=u(bl);data=u(JE);constructor(){}action(){this.snackBarRef.dismissWithAction()}get hasAction(){return!!this.data.action}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["simple-snack-bar"]],hostAttrs:[1,"mat-mdc-simple-snack-bar"],exportAs:["matSnackBar"],decls:3,vars:2,consts:[["matSnackBarLabel",""],["matSnackBarActions",""],["matButton","","matSnackBarAction","",3,"click"]],template:function(i,r){i&1&&(_(0,"div",0),E(1),g(),ce(2,cP,3,1,"div",1)),i&2&&(y(),Ye(" ",r.data.message,`
`),y(),de(r.hasAction?2:-1))},dependencies:[Gn,hP,fP,pP],styles:[`.mat-mdc-simple-snack-bar {
  display: flex;
}
.mat-mdc-simple-snack-bar .mat-mdc-snack-bar-label {
  max-height: 50vh;
  overflow: auto;
}
`],encapsulation:2,changeDetection:0})}return n})(),ev="_mat-snack-bar-enter",tv="_mat-snack-bar-exit",_P=(()=>{class n extends Zi{_ngZone=u(F);_elementRef=u(P);_changeDetectorRef=u(Ae);_platform=u(_e);_animationsDisabled=Fe();snackBarConfig=u(vs);_document=u(U);_trackedModals=new Set;_enterFallback;_exitFallback;_injector=u($);_announceDelay=150;_announceTimeoutId;_destroyed=!1;_portalOutlet;_onAnnounce=new k;_onExit=new k;_onEnter=new k;_animationState="void";_live;_label;_role;_liveElementId=u(Ie).getId("mat-snack-bar-container-live-");constructor(){super();let e=this.snackBarConfig;e.politeness==="assertive"&&!e.announcementMessage?this._live="assertive":e.politeness==="off"?this._live="off":this._live="polite",this._platform.FIREFOX&&(this._live==="polite"&&(this._role="status"),this._live==="assertive"&&(this._role="alert"))}attachComponentPortal(e){this._assertNotAttached();let i=this._portalOutlet.attachComponentPortal(e);return this._afterPortalAttached(),i}attachTemplatePortal(e){this._assertNotAttached();let i=this._portalOutlet.attachTemplatePortal(e);return this._afterPortalAttached(),i}attachDomPortal=e=>{this._assertNotAttached();let i=this._portalOutlet.attachDomPortal(e);return this._afterPortalAttached(),i};onAnimationEnd(e){e===tv?this._completeExit():e===ev&&(clearTimeout(this._enterFallback),this._ngZone.run(()=>{this._onEnter.next(),this._onEnter.complete()}))}enter(){this._destroyed||(this._animationState="visible",this._changeDetectorRef.markForCheck(),this._changeDetectorRef.detectChanges(),this._screenReaderAnnounce(),this._animationsDisabled?Qe(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(ev)))},{injector:this._injector}):(clearTimeout(this._enterFallback),this._enterFallback=setTimeout(()=>{this._elementRef.nativeElement.classList.add("mat-snack-bar-fallback-visible"),this.onAnimationEnd(ev)},200)))}exit(){return this._destroyed?$e(void 0):(this._ngZone.run(()=>{this._animationState="hidden",this._changeDetectorRef.markForCheck(),this._elementRef.nativeElement.setAttribute("mat-exit",""),clearTimeout(this._announceTimeoutId),this._animationsDisabled?Qe(()=>{this._ngZone.run(()=>queueMicrotask(()=>this.onAnimationEnd(tv)))},{injector:this._injector}):(clearTimeout(this._exitFallback),this._exitFallback=setTimeout(()=>this.onAnimationEnd(tv),200))}),this._onExit)}ngOnDestroy(){this._destroyed=!0,this._clearFromModals(),this._completeExit()}_completeExit(){clearTimeout(this._exitFallback),queueMicrotask(()=>{this._onExit.next(),this._onExit.complete()})}_afterPortalAttached(){let e=this._elementRef.nativeElement,i=this.snackBarConfig.panelClass;i&&(Array.isArray(i)?i.forEach(s=>e.classList.add(s)):e.classList.add(i)),this._exposeToModals();let r=this._label.nativeElement,o="mdc-snackbar__label";r.classList.toggle(o,!r.querySelector(`.${o}`))}_exposeToModals(){let e=this._liveElementId,i=this._document.querySelectorAll('body > .cdk-overlay-container [aria-modal="true"]');for(let r=0;r<i.length;r++){let o=i[r],s=o.getAttribute("aria-owns");this._trackedModals.add(o),s?s.indexOf(e)===-1&&o.setAttribute("aria-owns",s+" "+e):o.setAttribute("aria-owns",e)}}_clearFromModals(){this._trackedModals.forEach(e=>{let i=e.getAttribute("aria-owns");if(i){let r=i.replace(this._liveElementId,"").trim();r.length>0?e.setAttribute("aria-owns",r):e.removeAttribute("aria-owns")}}),this._trackedModals.clear()}_assertNotAttached(){this._portalOutlet.hasAttached()}_screenReaderAnnounce(){this._announceTimeoutId||this._ngZone.runOutsideAngular(()=>{this._announceTimeoutId=setTimeout(()=>{if(this._destroyed)return;let e=this._elementRef.nativeElement,i=e.querySelector("[aria-hidden]"),r=e.querySelector("[aria-live]");if(i&&r){let o=null;this._platform.isBrowser&&document.activeElement instanceof HTMLElement&&i.contains(document.activeElement)&&(o=document.activeElement),i.removeAttribute("aria-hidden"),r.appendChild(i),o?.focus(),this._onAnnounce.next(),this._onAnnounce.complete()}},this._announceDelay)})}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-snack-bar-container"]],viewQuery:function(i,r){if(i&1&&Te(Xi,7)(dP,7),i&2){let o;G(o=W())&&(r._portalOutlet=o.first),G(o=W())&&(r._label=o.first)}},hostAttrs:[1,"mdc-snackbar","mat-mdc-snack-bar-container"],hostVars:6,hostBindings:function(i,r){i&1&&V("animationend",function(s){return r.onAnimationEnd(s.animationName)})("animationcancel",function(s){return r.onAnimationEnd(s.animationName)}),i&2&&j("mat-snack-bar-container-enter",r._animationState==="visible")("mat-snack-bar-container-exit",r._animationState==="hidden")("mat-snack-bar-container-animations-enabled",!r._animationsDisabled)},features:[re],decls:6,vars:3,consts:[["label",""],[1,"mdc-snackbar__surface","mat-mdc-snackbar-surface"],[1,"mat-mdc-snack-bar-label"],["aria-hidden","true"],["cdkPortalOutlet",""]],template:function(i,r){i&1&&(_(0,"div",1)(1,"div",2,0)(3,"div",3),fe(4,uP,0,0,"ng-template",4),g(),Z(5,"div"),g()()),i&2&&(y(5),J("aria-live",r._live)("role",r._role)("id",r._liveElementId))},dependencies:[Xi],styles:[`@keyframes _mat-snack-bar-enter {
  from {
    transform: scale(0.8);
    opacity: 0;
  }
  to {
    transform: scale(1);
    opacity: 1;
  }
}
@keyframes _mat-snack-bar-exit {
  from {
    opacity: 1;
  }
  to {
    opacity: 0;
  }
}
.mat-mdc-snack-bar-container {
  display: flex;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
  margin: 8px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snack-bar-container {
  width: 100vw;
}

.mat-snack-bar-container-animations-enabled {
  opacity: 0;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-fallback-visible {
  opacity: 1;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-enter {
  animation: _mat-snack-bar-enter 150ms cubic-bezier(0, 0, 0.2, 1) forwards;
}
.mat-snack-bar-container-animations-enabled.mat-snack-bar-container-exit {
  animation: _mat-snack-bar-exit 75ms cubic-bezier(0.4, 0, 1, 1) forwards;
}

.mat-mdc-snackbar-surface {
  box-shadow: 0px 3px 5px -1px rgba(0, 0, 0, 0.2), 0px 6px 10px 0px rgba(0, 0, 0, 0.14), 0px 1px 18px 0px rgba(0, 0, 0, 0.12);
  display: flex;
  align-items: center;
  justify-content: flex-start;
  box-sizing: border-box;
  padding-left: 0;
  padding-right: 8px;
}
[dir=rtl] .mat-mdc-snackbar-surface {
  padding-right: 0;
  padding-left: 8px;
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  min-width: 344px;
  max-width: 672px;
}
.mat-mdc-snack-bar-handset .mat-mdc-snackbar-surface {
  width: 100%;
  min-width: 0;
}
@media (forced-colors: active) {
  .mat-mdc-snackbar-surface {
    outline: solid 1px;
  }
}
.mat-mdc-snack-bar-container .mat-mdc-snackbar-surface {
  color: var(--mat-snack-bar-supporting-text-color, var(--mat-sys-inverse-on-surface));
  border-radius: var(--mat-snack-bar-container-shape, var(--mat-sys-corner-extra-small));
  background-color: var(--mat-snack-bar-container-color, var(--mat-sys-inverse-surface));
}

.mdc-snackbar__label {
  width: 100%;
  flex-grow: 1;
  box-sizing: border-box;
  margin: 0;
  padding: 14px 8px 14px 16px;
}
[dir=rtl] .mdc-snackbar__label {
  padding-left: 8px;
  padding-right: 16px;
}
.mat-mdc-snack-bar-container .mdc-snackbar__label {
  font-family: var(--mat-snack-bar-supporting-text-font, var(--mat-sys-body-medium-font));
  font-size: var(--mat-snack-bar-supporting-text-size, var(--mat-sys-body-medium-size));
  font-weight: var(--mat-snack-bar-supporting-text-weight, var(--mat-sys-body-medium-weight));
  line-height: var(--mat-snack-bar-supporting-text-line-height, var(--mat-sys-body-medium-line-height));
}

.mat-mdc-snack-bar-actions {
  display: flex;
  flex-shrink: 0;
  align-items: center;
  box-sizing: border-box;
}

.mat-mdc-snack-bar-handset,
.mat-mdc-snack-bar-container,
.mat-mdc-snack-bar-label {
  flex: 1 1 auto;
}

.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled).mat-unthemed {
  color: var(--mat-snack-bar-button-color, var(--mat-sys-inverse-primary));
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) {
  --mat-button-text-state-layer-color: currentColor;
  --mat-button-text-ripple-color: currentColor;
}
.mat-mdc-snack-bar-container .mat-mdc-button.mat-mdc-snack-bar-action:not(:disabled) .mat-ripple-element {
  opacity: 0.1;
}
`],encapsulation:2})}return n})(),vP=new v("mat-snack-bar-default-options",{providedIn:"root",factory:()=>new vs}),ek=(()=>{class n{_live=u(Xa);_injector=u($);_breakpointObserver=u(hi);_parentSnackBar=u(n,{optional:!0,skipSelf:!0});_defaultConfig=u(vP);_animationsDisabled=Fe();_snackBarRefAtThisLevel=null;simpleSnackBarComponent=gP;snackBarContainerComponent=_P;handsetCssClass="mat-mdc-snack-bar-handset";get _openedSnackBarRef(){let e=this._parentSnackBar;return e?e._openedSnackBarRef:this._snackBarRefAtThisLevel}set _openedSnackBarRef(e){this._parentSnackBar?this._parentSnackBar._openedSnackBarRef=e:this._snackBarRefAtThisLevel=e}constructor(){}openFromComponent(e,i){return this._attach(e,i)}openFromTemplate(e,i){return this._attach(e,i)}open(e,i="",r){let o=M(M({},this._defaultConfig),r);return o.data={message:e,action:i},o.announcementMessage===e&&(o.announcementMessage=void 0),this.openFromComponent(this.simpleSnackBarComponent,o)}dismiss(){this._openedSnackBarRef&&this._openedSnackBarRef.dismiss()}ngOnDestroy(){this._snackBarRefAtThisLevel&&this._snackBarRefAtThisLevel.dismiss()}_attachSnackBarContainer(e,i){let r=i&&i.viewContainerRef&&i.viewContainerRef.injector,o=$.create({parent:r||this._injector,providers:[{provide:vs,useValue:i}]}),s=new In(this.snackBarContainerComponent,i.viewContainerRef,o),a=e.attach(s);return a.instance.snackBarConfig=i,a.instance}_attach(e,i){let r=M(M(M({},new vs),this._defaultConfig),i),o=this._createOverlay(r),s=this._attachSnackBarContainer(o,r),a=new bl(s,o);if(e instanceof Pe){let l=new gi(e,null,{$implicit:r.data,snackBarRef:a});a.instance=s.attachTemplatePortal(l)}else{let l=this._createInjector(r,a),c=new In(e,void 0,l),d=s.attachComponentPortal(c);a.instance=d.instance}return this._breakpointObserver.observe(Px.HandsetPortrait).pipe(Ce(o.detachments())).subscribe(l=>{o.overlayElement.classList.toggle(this.handsetCssClass,l.matches)}),r.announcementMessage&&s._onAnnounce.subscribe(()=>{this._live.announce(r.announcementMessage,r.politeness)}),this._animateSnackBar(a,r),this._openedSnackBarRef=a,this._openedSnackBarRef}_animateSnackBar(e,i){e.afterDismissed().subscribe(()=>{this._openedSnackBarRef==e&&(this._openedSnackBarRef=null),i.announcementMessage&&this._live.clear()}),i.duration&&i.duration>0&&e.afterOpened().subscribe(()=>e._dismissAfter(i.duration)),this._openedSnackBarRef?(this._openedSnackBarRef.afterDismissed().subscribe(()=>{e.containerInstance.enter()}),this._openedSnackBarRef.dismiss()):e.containerInstance.enter()}_createOverlay(e){let i=new _i;i.direction=e.direction;let r=er(this._injector),o=e.direction==="rtl",s=e.horizontalPosition==="left"||e.horizontalPosition==="start"&&!o||e.horizontalPosition==="end"&&o,a=!s&&e.horizontalPosition!=="center";return s?r.left("0"):a?r.right("0"):r.centerHorizontally(),e.verticalPosition==="top"?r.top("0"):r.bottom("0"),i.positionStrategy=r,i.disableAnimations=this._animationsDisabled,vi(this._injector,i)}_createInjector(e,i){let r=e&&e.viewContainerRef&&e.viewContainerRef.injector;return $.create({parent:r||this._injector,providers:[{provide:bl,useValue:i},{provide:JE,useValue:e.data}]})}static \u0275fac=function(i){return new(i||n)};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function tk(n){return Error(`Unable to find icon with the name "${n}"`)}function yP(){return Error("Could not find HttpClient for use with Angular Material icons. Please add provideHttpClient() to your providers.")}function nk(n){return Error(`The URL provided to MatIconRegistry was not trusted as a resource URL via Angular's DomSanitizer. Attempted URL was "${n}".`)}function ik(n){return Error(`The literal provided to MatIconRegistry was not trusted as safe HTML by Angular's DomSanitizer. Attempted literal was "${n}".`)}var bi=class{url;svgText;options;svgElement=null;constructor(t,e,i){this.url=t,this.svgText=e,this.options=i}},ok=(()=>{class n{_httpClient;_sanitizer;_errorHandler;_document;_svgIconConfigs=new Map;_iconSetConfigs=new Map;_cachedIconsByUrl=new Map;_inProgressUrlFetches=new Map;_fontCssClassesByAlias=new Map;_resolvers=[];_defaultFontSetClass=["material-icons","mat-ligature-font"];constructor(e,i,r,o){this._httpClient=e,this._sanitizer=i,this._errorHandler=o,this._document=r}addSvgIcon(e,i,r){return this.addSvgIconInNamespace("",e,i,r)}addSvgIconLiteral(e,i,r){return this.addSvgIconLiteralInNamespace("",e,i,r)}addSvgIconInNamespace(e,i,r,o){return this._addSvgIconConfig(e,i,new bi(r,null,o))}addSvgIconResolver(e){return this._resolvers.push(e),this}addSvgIconLiteralInNamespace(e,i,r,o){let s=this._sanitizer.sanitize(ct.HTML,r);if(!s)throw ik(r);let a=Gr(s);return this._addSvgIconConfig(e,i,new bi("",a,o))}addSvgIconSet(e,i){return this.addSvgIconSetInNamespace("",e,i)}addSvgIconSetLiteral(e,i){return this.addSvgIconSetLiteralInNamespace("",e,i)}addSvgIconSetInNamespace(e,i,r){return this._addSvgIconSetConfig(e,new bi(i,null,r))}addSvgIconSetLiteralInNamespace(e,i,r){let o=this._sanitizer.sanitize(ct.HTML,i);if(!o)throw ik(i);let s=Gr(o);return this._addSvgIconSetConfig(e,new bi("",s,r))}registerFontClassAlias(e,i=e){return this._fontCssClassesByAlias.set(e,i),this}classNameForFontAlias(e){return this._fontCssClassesByAlias.get(e)||e}setDefaultFontSetClass(...e){return this._defaultFontSetClass=e,this}getDefaultFontSetClass(){return this._defaultFontSetClass}getSvgIconFromUrl(e){let i=this._sanitizer.sanitize(ct.RESOURCE_URL,e);if(!i)throw nk(e);let r=this._cachedIconsByUrl.get(i);return r?$e(km(r)):this._loadSvgIconFromConfig(new bi(e,null)).pipe(pr(o=>this._cachedIconsByUrl.set(i,o)),ye(o=>km(o)))}getNamedSvgIcon(e,i=""){let r=rk(i,e),o=this._svgIconConfigs.get(r);if(o)return this._getSvgFromConfig(o);if(o=this._getIconConfigFromResolvers(i,e),o)return this._svgIconConfigs.set(r,o),this._getSvgFromConfig(o);let s=this._iconSetConfigs.get(i);return s?this._getSvgFromIconSetConfigs(e,s):Zm(tk(r))}ngOnDestroy(){this._resolvers=[],this._svgIconConfigs.clear(),this._iconSetConfigs.clear(),this._cachedIconsByUrl.clear()}_getSvgFromConfig(e){return e.svgText?$e(km(this._svgElementFromConfig(e))):this._loadSvgIconFromConfig(e).pipe(ye(i=>km(i)))}_getSvgFromIconSetConfigs(e,i){let r=this._extractIconWithNameFromAnySet(e,i);if(r)return $e(r);let o=i.filter(s=>!s.svgText).map(s=>this._loadSvgIconSetFromConfig(s).pipe(mc(a=>{let c=`Loading icon set URL: ${this._sanitizer.sanitize(ct.RESOURCE_URL,s.url)} failed: ${a.message}`;return this._errorHandler.handleError(new Error(c)),$e(null)})));return Is(o).pipe(ye(()=>{let s=this._extractIconWithNameFromAnySet(e,i);if(!s)throw tk(e);return s}))}_extractIconWithNameFromAnySet(e,i){for(let r=i.length-1;r>=0;r--){let o=i[r];if(o.svgText&&o.svgText.toString().indexOf(e)>-1){let s=this._svgElementFromConfig(o),a=this._extractSvgIconFromSet(s,e,o.options);if(a)return a}}return null}_loadSvgIconFromConfig(e){return this._fetchIcon(e).pipe(pr(i=>e.svgText=i),ye(()=>this._svgElementFromConfig(e)))}_loadSvgIconSetFromConfig(e){return e.svgText?$e(null):this._fetchIcon(e).pipe(pr(i=>e.svgText=i))}_extractSvgIconFromSet(e,i,r){let o=e.querySelector(`[id="${i}"]`);if(!o)return null;let s=o.cloneNode(!0);if(s.removeAttribute("id"),s.nodeName.toLowerCase()==="svg")return this._setSvgAttributes(s,r);if(s.nodeName.toLowerCase()==="symbol")return this._setSvgAttributes(this._toSvgElement(s),r);let a=this._svgElementFromString(Gr("<svg></svg>"));return a.appendChild(s),this._setSvgAttributes(a,r)}_svgElementFromString(e){let i=this._document.createElement("DIV");i.innerHTML=e;let r=i.querySelector("svg");if(!r)throw Error("<svg> tag not found");return r}_toSvgElement(e){let i=this._svgElementFromString(Gr("<svg></svg>")),r=e.attributes;for(let o=0;o<r.length;o++){let{name:s,value:a}=r[o];s!=="id"&&i.setAttribute(s,a)}for(let o=0;o<e.childNodes.length;o++)e.childNodes[o].nodeType===this._document.ELEMENT_NODE&&i.appendChild(e.childNodes[o].cloneNode(!0));return i}_setSvgAttributes(e,i){return e.setAttribute("fit",""),e.setAttribute("height","100%"),e.setAttribute("width","100%"),e.setAttribute("preserveAspectRatio","xMidYMid meet"),e.setAttribute("focusable","false"),i&&i.viewBox&&e.setAttribute("viewBox",i.viewBox),e}_fetchIcon(e){let{url:i,options:r}=e,o=r?.withCredentials??!1;if(!this._httpClient)throw yP();if(i==null)throw Error(`Cannot fetch icon from URL "${i}".`);let s=this._sanitizer.sanitize(ct.RESOURCE_URL,i);if(!s)throw nk(i);let a=this._inProgressUrlFetches.get(s);if(a)return a;let l=this._httpClient.get(s,{responseType:"text",withCredentials:o}).pipe(ye(c=>Gr(c)),fo(()=>this._inProgressUrlFetches.delete(s)),Ts());return this._inProgressUrlFetches.set(s,l),l}_addSvgIconConfig(e,i,r){return this._svgIconConfigs.set(rk(e,i),r),this}_addSvgIconSetConfig(e,i){let r=this._iconSetConfigs.get(e);return r?r.push(i):this._iconSetConfigs.set(e,[i]),this}_svgElementFromConfig(e){if(!e.svgElement){let i=this._svgElementFromString(e.svgText);this._setSvgAttributes(i,e.options),e.svgElement=i}return e.svgElement}_getIconConfigFromResolvers(e,i){for(let r=0;r<this._resolvers.length;r++){let o=this._resolvers[r](i,e);if(o)return wP(o)?new bi(o.url,null,o.options):new bi(o,null)}}static \u0275fac=function(i){return new(i||n)(B(ln,8),B(Ia),B(U,8),B(At))};static \u0275prov=w({token:n,factory:n.\u0275fac,providedIn:"root"})}return n})();function km(n){return n.cloneNode(!0)}function rk(n,t){return n+":"+t}function wP(n){return!!(n.url&&n.options)}var CP=["*"],DP=new v("MAT_ICON_DEFAULT_OPTIONS"),xP=new v("mat-icon-location",{providedIn:"root",factory:()=>{let n=u(U),t=n?n.location:null;return{getPathname:()=>t?t.pathname+t.search:""}}}),sk=["clip-path","color-profile","src","cursor","fill","filter","marker","marker-start","marker-mid","marker-end","mask","stroke"],EP=sk.map(n=>`[${n}]`).join(", "),kP=/^url\(['"]?#(.*?)['"]?\)$/,ak=(()=>{class n{_elementRef=u(P);_iconRegistry=u(ok);_location=u(xP);_errorHandler=u(At);_defaultColor;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;inline=!1;get svgIcon(){return this._svgIcon}set svgIcon(e){e!==this._svgIcon&&(e?this._updateSvgIcon(e):this._svgIcon&&this._clearSvgElement(),this._svgIcon=e)}_svgIcon;get fontSet(){return this._fontSet}set fontSet(e){let i=this._cleanupFontValue(e);i!==this._fontSet&&(this._fontSet=i,this._updateFontIconClasses())}_fontSet;get fontIcon(){return this._fontIcon}set fontIcon(e){let i=this._cleanupFontValue(e);i!==this._fontIcon&&(this._fontIcon=i,this._updateFontIconClasses())}_fontIcon;_previousFontSetClass=[];_previousFontIconClass;_svgName=null;_svgNamespace=null;_previousPath;_elementsWithExternalReferences;_currentIconFetch=ge.EMPTY;constructor(){let e=u(new an("aria-hidden"),{optional:!0}),i=u(DP,{optional:!0});i&&(i.color&&(this.color=this._defaultColor=i.color),i.fontSet&&(this.fontSet=i.fontSet)),e||this._elementRef.nativeElement.setAttribute("aria-hidden","true")}_splitIconName(e){if(!e)return["",""];let i=e.split(":");switch(i.length){case 1:return["",i[0]];case 2:return i;default:throw Error(`Invalid icon name: "${e}"`)}}ngOnInit(){this._updateFontIconClasses()}ngAfterViewChecked(){let e=this._elementsWithExternalReferences;if(e&&e.size){let i=this._location.getPathname();i!==this._previousPath&&(this._previousPath=i,this._prependPathToReferences(i))}}ngOnDestroy(){this._currentIconFetch.unsubscribe(),this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear()}_usingFontIcon(){return!this.svgIcon}_setSvgElement(e){this._clearSvgElement();let i=this._location.getPathname();this._previousPath=i,this._cacheChildrenWithExternalReferences(e),this._prependPathToReferences(i),this._elementRef.nativeElement.appendChild(e)}_clearSvgElement(){let e=this._elementRef.nativeElement,i=e.childNodes.length;for(this._elementsWithExternalReferences&&this._elementsWithExternalReferences.clear();i--;){let r=e.childNodes[i];(r.nodeType!==1||r.nodeName.toLowerCase()==="svg")&&r.remove()}}_updateFontIconClasses(){if(!this._usingFontIcon())return;let e=this._elementRef.nativeElement,i=(this.fontSet?this._iconRegistry.classNameForFontAlias(this.fontSet).split(/ +/):this._iconRegistry.getDefaultFontSetClass()).filter(r=>r.length>0);this._previousFontSetClass.forEach(r=>e.classList.remove(r)),i.forEach(r=>e.classList.add(r)),this._previousFontSetClass=i,this.fontIcon!==this._previousFontIconClass&&!i.includes("mat-ligature-font")&&(this._previousFontIconClass&&e.classList.remove(this._previousFontIconClass),this.fontIcon&&e.classList.add(this.fontIcon),this._previousFontIconClass=this.fontIcon)}_cleanupFontValue(e){return typeof e=="string"?e.trim().split(" ")[0]:e}_prependPathToReferences(e){let i=this._elementsWithExternalReferences;i&&i.forEach((r,o)=>{r.forEach(s=>{o.setAttribute(s.name,`url('${e}#${s.value}')`)})})}_cacheChildrenWithExternalReferences(e){let i=e.querySelectorAll(EP),r=this._elementsWithExternalReferences=this._elementsWithExternalReferences||new Map;for(let o=0;o<i.length;o++)sk.forEach(s=>{let a=i[o],l=a.getAttribute(s),c=l?l.match(kP):null;if(c){let d=r.get(a);d||(d=[],r.set(a,d)),d.push({name:s,value:c[1]})}})}_updateSvgIcon(e){if(this._svgNamespace=null,this._svgName=null,this._currentIconFetch.unsubscribe(),e){let[i,r]=this._splitIconName(e);i&&(this._svgNamespace=i),r&&(this._svgName=r),this._currentIconFetch=this._iconRegistry.getNamedSvgIcon(r,i).pipe(Vt(1)).subscribe(o=>this._setSvgElement(o),o=>{let s=`Error retrieving icon ${i}:${r}! ${o.message}`;this._errorHandler.handleError(new Error(s))})}}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-icon"]],hostAttrs:["role","img",1,"mat-icon","notranslate"],hostVars:10,hostBindings:function(i,r){i&2&&(J("data-mat-icon-type",r._usingFontIcon()?"font":"svg")("data-mat-icon-name",r._svgName||r.fontIcon)("data-mat-icon-namespace",r._svgNamespace||r.fontSet)("fontIcon",r._usingFontIcon()?r.fontIcon:null),St(r.color?"mat-"+r.color:""),j("mat-icon-inline",r.inline)("mat-icon-no-color",r.color!=="primary"&&r.color!=="accent"&&r.color!=="warn"))},inputs:{color:"color",inline:[2,"inline","inline",N],svgIcon:"svgIcon",fontSet:"fontSet",fontIcon:"fontIcon"},exportAs:["matIcon"],ngContentSelectors:CP,decls:1,vars:0,template:function(i,r){i&1&&(Ee(),ie(0))},styles:[`mat-icon, mat-icon.mat-primary, mat-icon.mat-accent, mat-icon.mat-warn {
  color: var(--mat-icon-color, inherit);
}

.mat-icon {
  -webkit-user-select: none;
  user-select: none;
  background-repeat: no-repeat;
  display: inline-block;
  fill: currentColor;
  height: 24px;
  width: 24px;
  overflow: hidden;
}
.mat-icon.mat-icon-inline {
  font-size: inherit;
  height: inherit;
  line-height: inherit;
  width: inherit;
}
.mat-icon.mat-ligature-font[fontIcon]::before {
  content: attr(fontIcon);
}

[dir=rtl] .mat-icon-rtl-mirror {
  transform: scale(-1, 1);
}

.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon {
  display: block;
}
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-prefix .mat-icon-button .mat-icon,
.mat-form-field:not(.mat-form-field-appearance-legacy) .mat-form-field-suffix .mat-icon-button .mat-icon {
  margin: auto;
}
`],encapsulation:2,changeDetection:0})}return n})(),yl=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe]})}return n})();var IP=["button"],MP=["*"];function TP(n,t){if(n&1&&(_(0,"div",2),Z(1,"mat-pseudo-checkbox",6),g()),n&2){let e=H();y(),R("disabled",e.disabled)}}var lk=new v("MAT_BUTTON_TOGGLE_DEFAULT_OPTIONS",{providedIn:"root",factory:()=>({hideSingleSelectionIndicator:!1,hideMultipleSelectionIndicator:!1,disabledInteractive:!1})}),ck=new v("MatButtonToggleGroup"),AP={provide:Ur,useExisting:bt(()=>nv),multi:!0},Sm=class{source;value;constructor(t,e){this.source=t,this.value=e}},nv=(()=>{class n{_changeDetector=u(Ae);_dir=u(st,{optional:!0});_multiple=!1;_disabled=!1;_disabledInteractive=!1;_selectionModel;_rawValue;_controlValueAccessorChangeFn=()=>{};_onTouched=()=>{};_buttonToggles;appearance;get name(){return this._name}set name(e){this._name=e,this._markButtonsForCheck()}_name=u(Ie).getId("mat-button-toggle-group-");vertical=!1;get value(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e.map(i=>i.value):e[0]?e[0].value:void 0}set value(e){this._setSelectionByValue(e),this.valueChange.emit(this.value)}valueChange=new z;get selected(){let e=this._selectionModel?this._selectionModel.selected:[];return this.multiple?e:e[0]||null}get multiple(){return this._multiple}set multiple(e){this._multiple=e,this._markButtonsForCheck()}get disabled(){return this._disabled}set disabled(e){this._disabled=e,this._markButtonsForCheck()}get disabledInteractive(){return this._disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e,this._markButtonsForCheck()}get dir(){return this._dir&&this._dir.value==="rtl"?"rtl":"ltr"}change=new z;get hideSingleSelectionIndicator(){return this._hideSingleSelectionIndicator}set hideSingleSelectionIndicator(e){this._hideSingleSelectionIndicator=e,this._markButtonsForCheck()}_hideSingleSelectionIndicator;get hideMultipleSelectionIndicator(){return this._hideMultipleSelectionIndicator}set hideMultipleSelectionIndicator(e){this._hideMultipleSelectionIndicator=e,this._markButtonsForCheck()}_hideMultipleSelectionIndicator;constructor(){let e=u(lk,{optional:!0});this.appearance=e&&e.appearance?e.appearance:"standard",this._hideSingleSelectionIndicator=e?.hideSingleSelectionIndicator??!1,this._hideMultipleSelectionIndicator=e?.hideMultipleSelectionIndicator??!1}ngOnInit(){this._selectionModel=new Kr(this.multiple,void 0,!1)}ngAfterContentInit(){this._selectionModel.select(...this._buttonToggles.filter(e=>e.checked)),this.multiple||this._initializeTabIndex()}writeValue(e){this.value=e,this._changeDetector.markForCheck()}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}_keydown(e){if(this.multiple||this.disabled||ut(e))return;let r=e.target.id,o=this._buttonToggles.toArray().findIndex(a=>a.buttonId===r),s=null;switch(e.keyCode){case 32:case 13:s=this._buttonToggles.get(o)||null;break;case 38:s=this._getNextButton(o,-1);break;case 37:s=this._getNextButton(o,this.dir==="ltr"?-1:1);break;case 40:s=this._getNextButton(o,1);break;case 39:s=this._getNextButton(o,this.dir==="ltr"?1:-1);break;default:return}s&&(e.preventDefault(),s._onButtonClick(),s.focus())}_emitChangeEvent(e){let i=new Sm(e,this.value);this._rawValue=i.value,this._controlValueAccessorChangeFn(i.value),this.change.emit(i)}_syncButtonToggle(e,i,r=!1,o=!1){!this.multiple&&this.selected&&!e.checked&&(this.selected.checked=!1),this._selectionModel?i?this._selectionModel.select(e):this._selectionModel.deselect(e):o=!0,o?Promise.resolve().then(()=>this._updateModelValue(e,r)):this._updateModelValue(e,r)}_isSelected(e){return this._selectionModel&&this._selectionModel.isSelected(e)}_isPrechecked(e){return typeof this._rawValue>"u"?!1:this.multiple&&Array.isArray(this._rawValue)?this._rawValue.some(i=>e.value!=null&&i===e.value):e.value===this._rawValue}_initializeTabIndex(){if(this._buttonToggles.forEach(e=>{e.tabIndex=-1}),this.selected)this.selected.tabIndex=0;else for(let e=0;e<this._buttonToggles.length;e++){let i=this._buttonToggles.get(e);if(!i.disabled){i.tabIndex=0;break}}}_getNextButton(e,i){let r=this._buttonToggles;for(let o=1;o<=r.length;o++){let s=(e+i*o+r.length)%r.length,a=r.get(s);if(a&&!a.disabled)return a}return null}_setSelectionByValue(e){if(this._rawValue=e,!this._buttonToggles)return;let i=this._buttonToggles.toArray();if(this.multiple&&e?(Array.isArray(e),this._clearSelection(),e.forEach(r=>this._selectValue(r,i))):(this._clearSelection(),this._selectValue(e,i)),!this.multiple&&i.every(r=>r.tabIndex===-1)){for(let r of i)if(!r.disabled){r.tabIndex=0;break}}}_clearSelection(){this._selectionModel.clear(),this._buttonToggles.forEach(e=>{e.checked=!1,this.multiple||(e.tabIndex=-1)})}_selectValue(e,i){for(let r of i)if(r.value===e){r.checked=!0,this._selectionModel.select(r),this.multiple||(r.tabIndex=0);break}}_updateModelValue(e,i){i&&this._emitChangeEvent(e),this.valueChange.emit(this.value)}_markButtonsForCheck(){this._buttonToggles?.forEach(e=>e._markForCheck())}static \u0275fac=function(i){return new(i||n)};static \u0275dir=T({type:n,selectors:[["mat-button-toggle-group"]],contentQueries:function(i,r,o){if(i&1&&kt(o,wl,5),i&2){let s;G(s=W())&&(r._buttonToggles=s)}},hostAttrs:[1,"mat-button-toggle-group"],hostVars:6,hostBindings:function(i,r){i&1&&V("keydown",function(s){return r._keydown(s)}),i&2&&(J("role",r.multiple?"group":"radiogroup")("aria-disabled",r.disabled),j("mat-button-toggle-vertical",r.vertical)("mat-button-toggle-group-appearance-standard",r.appearance==="standard"))},inputs:{appearance:"appearance",name:"name",vertical:[2,"vertical","vertical",N],value:"value",multiple:[2,"multiple","multiple",N],disabled:[2,"disabled","disabled",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N],hideSingleSelectionIndicator:[2,"hideSingleSelectionIndicator","hideSingleSelectionIndicator",N],hideMultipleSelectionIndicator:[2,"hideMultipleSelectionIndicator","hideMultipleSelectionIndicator",N]},outputs:{valueChange:"valueChange",change:"change"},exportAs:["matButtonToggleGroup"],features:[ve([AP,{provide:ck,useExisting:n}])]})}return n})(),wl=(()=>{class n{_changeDetectorRef=u(Ae);_elementRef=u(P);_focusMonitor=u(mi);_idGenerator=u(Ie);_animationDisabled=Fe();_checked=!1;ariaLabel;ariaLabelledby=null;_buttonElement;buttonToggleGroup;get buttonId(){return`${this.id}-button`}id;name;value;get tabIndex(){return this._tabIndex()}set tabIndex(e){this._tabIndex.set(e)}_tabIndex;disableRipple=!1;get appearance(){return this.buttonToggleGroup?this.buttonToggleGroup.appearance:this._appearance}set appearance(e){this._appearance=e}_appearance;get checked(){return this.buttonToggleGroup?this.buttonToggleGroup._isSelected(this):this._checked}set checked(e){e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&this.buttonToggleGroup._syncButtonToggle(this,this._checked),this._changeDetectorRef.markForCheck())}get disabled(){return this._disabled||this.buttonToggleGroup&&this.buttonToggleGroup.disabled}set disabled(e){this._disabled=e}_disabled=!1;get disabledInteractive(){return this._disabledInteractive||this.buttonToggleGroup!==null&&this.buttonToggleGroup.disabledInteractive}set disabledInteractive(e){this._disabledInteractive=e}_disabledInteractive;change=new z;constructor(){u(it).load(qi);let e=u(ck,{optional:!0}),i=u(new an("tabindex"),{optional:!0})||"",r=u(lk,{optional:!0});this._tabIndex=Oe(parseInt(i)||0),this.buttonToggleGroup=e,this._appearance=r&&r.appearance?r.appearance:"standard",this._disabledInteractive=r?.disabledInteractive??!1}ngOnInit(){let e=this.buttonToggleGroup;this.id=this.id||this._idGenerator.getId("mat-button-toggle-"),e&&(e._isPrechecked(this)?this.checked=!0:e._isSelected(this)!==this._checked&&e._syncButtonToggle(this,this._checked))}ngAfterViewInit(){this._animationDisabled||this._elementRef.nativeElement.classList.add("mat-button-toggle-animations-enabled"),this._focusMonitor.monitor(this._elementRef,!0)}ngOnDestroy(){let e=this.buttonToggleGroup;this._focusMonitor.stopMonitoring(this._elementRef),e&&e._isSelected(this)&&e._syncButtonToggle(this,!1,!1,!0)}focus(e){this._buttonElement.nativeElement.focus(e)}_onButtonClick(){if(this.disabled)return;let e=this.isSingleSelector()?!0:!this._checked;if(e!==this._checked&&(this._checked=e,this.buttonToggleGroup&&(this.buttonToggleGroup._syncButtonToggle(this,this._checked,!0),this.buttonToggleGroup._onTouched())),this.isSingleSelector()){let i=this.buttonToggleGroup._buttonToggles.find(r=>r.tabIndex===0);i&&(i.tabIndex=-1),this.tabIndex=0}this.change.emit(new Sm(this,this.value))}_markForCheck(){this._changeDetectorRef.markForCheck()}_getButtonName(){return this.isSingleSelector()?this.buttonToggleGroup.name:this.name||null}isSingleSelector(){return this.buttonToggleGroup&&!this.buttonToggleGroup.multiple}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-button-toggle"]],viewQuery:function(i,r){if(i&1&&Te(IP,5),i&2){let o;G(o=W())&&(r._buttonElement=o.first)}},hostAttrs:["role","presentation",1,"mat-button-toggle"],hostVars:14,hostBindings:function(i,r){i&1&&V("focus",function(){return r.focus()}),i&2&&(J("aria-label",null)("aria-labelledby",null)("id",r.id)("name",null),j("mat-button-toggle-standalone",!r.buttonToggleGroup)("mat-button-toggle-checked",r.checked)("mat-button-toggle-disabled",r.disabled)("mat-button-toggle-disabled-interactive",r.disabledInteractive)("mat-button-toggle-appearance-standard",r.appearance==="standard"))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],id:"id",name:"name",value:"value",tabIndex:"tabIndex",disableRipple:[2,"disableRipple","disableRipple",N],appearance:"appearance",checked:[2,"checked","checked",N],disabled:[2,"disabled","disabled",N],disabledInteractive:[2,"disabledInteractive","disabledInteractive",N]},outputs:{change:"change"},exportAs:["matButtonToggle"],ngContentSelectors:MP,decls:7,vars:13,consts:[["button",""],["type","button",1,"mat-button-toggle-button","mat-focus-indicator",3,"click","id","disabled"],[1,"mat-button-toggle-checkbox-wrapper"],[1,"mat-button-toggle-label-content"],[1,"mat-button-toggle-focus-overlay"],["matRipple","",1,"mat-button-toggle-ripple",3,"matRippleTrigger","matRippleDisabled"],["state","checked","aria-hidden","true","appearance","minimal",3,"disabled"]],template:function(i,r){if(i&1&&(Ee(),_(0,"button",1,0),V("click",function(){return r._onButtonClick()}),ce(2,TP,2,1,"div",2),_(3,"span",3),ie(4),g()(),Z(5,"span",4)(6,"span",5)),i&2){let o=dt(1);R("id",r.buttonId)("disabled",r.disabled&&!r.disabledInteractive||null),J("role",r.isSingleSelector()?"radio":"button")("tabindex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex)("aria-pressed",r.isSingleSelector()?null:r.checked)("aria-checked",r.isSingleSelector()?r.checked:null)("name",r._getButtonName())("aria-label",r.ariaLabel)("aria-labelledby",r.ariaLabelledby)("aria-disabled",r.disabled&&r.disabledInteractive?"true":null),y(2),de(r.buttonToggleGroup&&(!r.buttonToggleGroup.multiple&&!r.buttonToggleGroup.hideSingleSelectionIndicator||r.buttonToggleGroup.multiple&&!r.buttonToggleGroup.hideMultipleSelectionIndicator)?2:-1),y(4),R("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)}},dependencies:[ls,sm],styles:[`.mat-button-toggle-standalone,
.mat-button-toggle-group {
  position: relative;
  display: inline-flex;
  flex-direction: row;
  white-space: nowrap;
  overflow: hidden;
  -webkit-tap-highlight-color: transparent;
  border-radius: var(--mat-button-toggle-legacy-shape);
  transform: translateZ(0);
}
.mat-button-toggle-standalone:not([class*=mat-elevation-z]),
.mat-button-toggle-group:not([class*=mat-elevation-z]) {
  box-shadow: 0px 3px 1px -2px rgba(0, 0, 0, 0.2), 0px 2px 2px 0px rgba(0, 0, 0, 0.14), 0px 1px 5px 0px rgba(0, 0, 0, 0.12);
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone,
  .mat-button-toggle-group {
    outline: solid 1px;
  }
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
.mat-button-toggle-group-appearance-standard {
  border-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
  border: solid 1px var(--mat-button-toggle-divider-color, var(--mat-sys-outline));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard .mat-pseudo-checkbox,
.mat-button-toggle-group-appearance-standard .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-button-toggle-selected-state-text-color, var(--mat-sys-on-secondary-container));
}
.mat-button-toggle-standalone.mat-button-toggle-appearance-standard:not([class*=mat-elevation-z]),
.mat-button-toggle-group-appearance-standard:not([class*=mat-elevation-z]) {
  box-shadow: none;
}
@media (forced-colors: active) {
  .mat-button-toggle-standalone.mat-button-toggle-appearance-standard,
  .mat-button-toggle-group-appearance-standard {
    outline: 0;
  }
}

.mat-button-toggle-vertical {
  flex-direction: column;
}
.mat-button-toggle-vertical .mat-button-toggle-label-content {
  display: block;
}

.mat-button-toggle {
  white-space: nowrap;
  position: relative;
  color: var(--mat-button-toggle-legacy-text-color);
  font-family: var(--mat-button-toggle-legacy-label-text-font);
  font-size: var(--mat-button-toggle-legacy-label-text-size);
  line-height: var(--mat-button-toggle-legacy-label-text-line-height);
  font-weight: var(--mat-button-toggle-legacy-label-text-weight);
  letter-spacing: var(--mat-button-toggle-legacy-label-text-tracking);
  --mat-pseudo-checkbox-minimal-selected-checkmark-color: var(--mat-button-toggle-legacy-selected-state-text-color);
}
.mat-button-toggle.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--mat-button-toggle-legacy-focus-state-layer-opacity);
}
.mat-button-toggle .mat-icon svg {
  vertical-align: top;
}

.mat-button-toggle-checkbox-wrapper {
  display: inline-block;
  justify-content: flex-start;
  align-items: center;
  width: 0;
  height: 18px;
  line-height: 18px;
  overflow: hidden;
  box-sizing: border-box;
  position: absolute;
  top: 50%;
  left: 16px;
  transform: translate3d(0, -50%, 0);
}
[dir=rtl] .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 16px;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: 12px;
}
[dir=rtl] .mat-button-toggle-appearance-standard .mat-button-toggle-checkbox-wrapper {
  left: auto;
  right: 12px;
}
.mat-button-toggle-checked .mat-button-toggle-checkbox-wrapper {
  width: 18px;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-checkbox-wrapper {
  transition: width 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-checkbox-wrapper {
  transition: none;
}

.mat-button-toggle-checked {
  color: var(--mat-button-toggle-legacy-selected-state-text-color);
  background-color: var(--mat-button-toggle-legacy-selected-state-background-color);
}

.mat-button-toggle-disabled {
  pointer-events: none;
  color: var(--mat-button-toggle-legacy-disabled-state-text-color);
  background-color: var(--mat-button-toggle-legacy-disabled-state-background-color);
  --mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--mat-button-toggle-legacy-disabled-state-text-color);
}
.mat-button-toggle-disabled.mat-button-toggle-checked {
  background-color: var(--mat-button-toggle-legacy-disabled-selected-state-background-color);
}

.mat-button-toggle-disabled-interactive {
  pointer-events: auto;
}

.mat-button-toggle-appearance-standard {
  color: var(--mat-button-toggle-text-color, var(--mat-sys-on-surface));
  background-color: var(--mat-button-toggle-background-color, transparent);
  font-family: var(--mat-button-toggle-label-text-font, var(--mat-sys-label-large-font));
  font-size: var(--mat-button-toggle-label-text-size, var(--mat-sys-label-large-size));
  line-height: var(--mat-button-toggle-label-text-line-height, var(--mat-sys-label-large-line-height));
  font-weight: var(--mat-button-toggle-label-text-weight, var(--mat-sys-label-large-weight));
  letter-spacing: var(--mat-button-toggle-label-text-tracking, var(--mat-sys-label-large-tracking));
}
.mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: solid 1px var(--mat-button-toggle-divider-color, var(--mat-sys-outline));
}
[dir=rtl] .mat-button-toggle-group-appearance-standard .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: solid 1px var(--mat-button-toggle-divider-color, var(--mat-sys-outline));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle-appearance-standard + .mat-button-toggle-appearance-standard {
  border-left: none;
  border-right: none;
  border-top: solid 1px var(--mat-button-toggle-divider-color, var(--mat-sys-outline));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-checked {
  color: var(--mat-button-toggle-selected-state-text-color, var(--mat-sys-on-secondary-container));
  background-color: var(--mat-button-toggle-selected-state-background-color, var(--mat-sys-secondary-container));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled {
  color: var(--mat-button-toggle-disabled-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-toggle-disabled-state-background-color, transparent);
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled .mat-pseudo-checkbox {
  --mat-pseudo-checkbox-minimal-disabled-selected-checkmark-color: var(--mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
.mat-button-toggle-appearance-standard.mat-button-toggle-disabled.mat-button-toggle-checked {
  color: var(--mat-button-toggle-disabled-selected-state-text-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  background-color: var(--mat-button-toggle-disabled-selected-state-background-color, color-mix(in srgb, var(--mat-sys-on-surface) 12%, transparent));
}
.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
  background-color: var(--mat-button-toggle-state-layer-color, var(--mat-sys-on-surface));
}
.mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
  opacity: var(--mat-button-toggle-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
}
.mat-button-toggle-appearance-standard.cdk-keyboard-focused .mat-button-toggle-focus-overlay {
  opacity: var(--mat-button-toggle-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
}
@media (hover: none) {
  .mat-button-toggle-appearance-standard:hover .mat-button-toggle-focus-overlay {
    display: none;
  }
}

.mat-button-toggle-label-content {
  -webkit-user-select: none;
  user-select: none;
  display: inline-block;
  padding: 0 16px;
  line-height: var(--mat-button-toggle-legacy-height);
  position: relative;
}
.mat-button-toggle-appearance-standard .mat-button-toggle-label-content {
  padding: 0 12px;
  line-height: var(--mat-button-toggle-height, 40px);
}

.mat-button-toggle-label-content > * {
  vertical-align: middle;
}

.mat-button-toggle-focus-overlay {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: inherit;
  pointer-events: none;
  opacity: 0;
  background-color: var(--mat-button-toggle-legacy-state-layer-color);
}

@media (forced-colors: active) {
  .mat-button-toggle-checked .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
    opacity: 0.5;
    height: 0;
  }
  .mat-button-toggle-checked:hover .mat-button-toggle-focus-overlay {
    opacity: 0.6;
  }
  .mat-button-toggle-checked.mat-button-toggle-appearance-standard .mat-button-toggle-focus-overlay {
    border-bottom: solid 500px;
  }
}
.mat-button-toggle .mat-button-toggle-ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  pointer-events: none;
}

.mat-button-toggle-button {
  border: 0;
  background: none;
  color: inherit;
  padding: 0;
  margin: 0;
  font: inherit;
  outline: none;
  width: 100%;
  cursor: pointer;
}
.mat-button-toggle-animations-enabled .mat-button-toggle-button {
  transition: padding 150ms 45ms cubic-bezier(0.4, 0, 0.2, 1);
}
.mat-button-toggle-vertical .mat-button-toggle-button {
  transition: none;
}
.mat-button-toggle-disabled .mat-button-toggle-button {
  cursor: default;
}
.mat-button-toggle-button::-moz-focus-inner {
  border: 0;
}
.mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 30px;
}
[dir=rtl] .mat-button-toggle-checked .mat-button-toggle-button:has(.mat-button-toggle-checkbox-wrapper) {
  padding-left: 0;
  padding-right: 30px;
}

.mat-button-toggle-standalone.mat-button-toggle-appearance-standard {
  --mat-focus-indicator-border-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
  border-bottom-right-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard:not(.mat-button-toggle-vertical) .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-left-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
}

.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:last-of-type .mat-button-toggle-button::before {
  border-bottom-right-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
  border-bottom-left-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
}
.mat-button-toggle-group-appearance-standard.mat-button-toggle-vertical .mat-button-toggle:first-of-type .mat-button-toggle-button::before {
  border-top-right-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
  border-top-left-radius: var(--mat-button-toggle-shape, var(--mat-sys-corner-extra-large));
}
`],encapsulation:2,changeDetection:0})}return n})();var OP=["determinateSpinner"];function NP(n,t){if(n&1&&(yt(),_(0,"svg",11),Z(1,"circle",12),g()),n&2){let e=H();J("viewBox",e._viewBox()),y(),Un("stroke-dasharray",e._strokeCircumference(),"px")("stroke-dashoffset",e._strokeCircumference()/2,"px")("stroke-width",e._circleStrokeWidth(),"%"),J("r",e._circleRadius())}}var FP=new v("mat-progress-spinner-default-options",{providedIn:"root",factory:()=>({diameter:dk})}),dk=100,PP=10,Im=(()=>{class n{_elementRef=u(P);_noopAnimations;get color(){return this._color||this._defaultColor}set color(e){this._color=e}_color;_defaultColor="primary";_determinateCircle;constructor(){let e=u(FP),i=g_(),r=this._elementRef.nativeElement;this._noopAnimations=i==="di-disabled"&&!!e&&!e._forceAnimations,this.mode=r.nodeName.toLowerCase()==="mat-spinner"?"indeterminate":"determinate",!this._noopAnimations&&i==="reduced-motion"&&r.classList.add("mat-progress-spinner-reduced-motion"),e&&(e.color&&(this.color=this._defaultColor=e.color),e.diameter&&(this.diameter=e.diameter),e.strokeWidth&&(this.strokeWidth=e.strokeWidth))}mode;get value(){return this.mode==="determinate"?this._value:0}set value(e){this._value=Math.max(0,Math.min(100,e||0))}_value=0;get diameter(){return this._diameter}set diameter(e){this._diameter=e||0}_diameter=dk;get strokeWidth(){return this._strokeWidth??this.diameter/10}set strokeWidth(e){this._strokeWidth=e||0}_strokeWidth;_circleRadius(){return(this.diameter-PP)/2}_viewBox(){let e=this._circleRadius()*2+this.strokeWidth;return`0 0 ${e} ${e}`}_strokeCircumference(){return 2*Math.PI*this._circleRadius()}_strokeDashOffset(){return this.mode==="determinate"?this._strokeCircumference()*(100-this._value)/100:null}_circleStrokeWidth(){return this.strokeWidth/this.diameter*100}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-progress-spinner"],["mat-spinner"]],viewQuery:function(i,r){if(i&1&&Te(OP,5),i&2){let o;G(o=W())&&(r._determinateCircle=o.first)}},hostAttrs:["role","progressbar","tabindex","-1",1,"mat-mdc-progress-spinner","mdc-circular-progress"],hostVars:18,hostBindings:function(i,r){i&2&&(J("aria-valuemin",0)("aria-valuemax",100)("aria-valuenow",r.mode==="determinate"?r.value:null)("mode",r.mode),St("mat-"+r.color),Un("width",r.diameter,"px")("height",r.diameter,"px")("--mat-progress-spinner-size",r.diameter+"px")("--mat-progress-spinner-active-indicator-width",r.diameter+"px"),j("_mat-animation-noopable",r._noopAnimations)("mdc-circular-progress--indeterminate",r.mode==="indeterminate"))},inputs:{color:"color",mode:"mode",value:[2,"value","value",ht],diameter:[2,"diameter","diameter",ht],strokeWidth:[2,"strokeWidth","strokeWidth",ht]},exportAs:["matProgressSpinner"],decls:14,vars:11,consts:[["circle",""],["determinateSpinner",""],["aria-hidden","true",1,"mdc-circular-progress__determinate-container"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__determinate-circle-graphic"],["cx","50%","cy","50%",1,"mdc-circular-progress__determinate-circle"],["aria-hidden","true",1,"mdc-circular-progress__indeterminate-container"],[1,"mdc-circular-progress__spinner-layer"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-left"],[3,"ngTemplateOutlet"],[1,"mdc-circular-progress__gap-patch"],[1,"mdc-circular-progress__circle-clipper","mdc-circular-progress__circle-right"],["xmlns","http://www.w3.org/2000/svg","focusable","false",1,"mdc-circular-progress__indeterminate-circle-graphic"],["cx","50%","cy","50%"]],template:function(i,r){if(i&1&&(fe(0,NP,2,8,"ng-template",null,0,Vo),_(2,"div",2,1),yt(),_(4,"svg",3),Z(5,"circle",4),g()(),ni(),_(6,"div",5)(7,"div",6)(8,"div",7),nt(9,8),g(),_(10,"div",9),nt(11,8),g(),_(12,"div",10),nt(13,8),g()()()),i&2){let o=dt(1);y(4),J("viewBox",r._viewBox()),y(),Un("stroke-dasharray",r._strokeCircumference(),"px")("stroke-dashoffset",r._strokeDashOffset(),"px")("stroke-width",r._circleStrokeWidth(),"%"),J("r",r._circleRadius()),y(4),R("ngTemplateOutlet",o),y(2),R("ngTemplateOutlet",o),y(2),R("ngTemplateOutlet",o)}},dependencies:[va],styles:[`.mat-mdc-progress-spinner {
  --mat-progress-spinner-animation-multiplier: 1;
  display: block;
  overflow: hidden;
  line-height: 0;
  position: relative;
  direction: ltr;
  transition: opacity 250ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mat-mdc-progress-spinner circle {
  stroke-width: var(--mat-progress-spinner-active-indicator-width, 4px);
}
.mat-mdc-progress-spinner._mat-animation-noopable, .mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__determinate-circle {
  transition: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-circle-graphic,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__spinner-layer,
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container {
  animation: none !important;
}
.mat-mdc-progress-spinner._mat-animation-noopable .mdc-circular-progress__indeterminate-container circle {
  stroke-dasharray: 0 !important;
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic,
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle {
    stroke: currentColor;
    stroke: CanvasText;
  }
}

.mat-progress-spinner-reduced-motion {
  --mat-progress-spinner-animation-multiplier: 1.25;
}

.mdc-circular-progress__determinate-container,
.mdc-circular-progress__indeterminate-circle-graphic,
.mdc-circular-progress__indeterminate-container,
.mdc-circular-progress__spinner-layer {
  position: absolute;
  width: 100%;
  height: 100%;
}

.mdc-circular-progress__determinate-container {
  transform: rotate(-90deg);
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__determinate-container {
  opacity: 0;
}

.mdc-circular-progress__indeterminate-container {
  font-size: 0;
  letter-spacing: 0;
  white-space: nowrap;
  opacity: 0;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__indeterminate-container {
  opacity: 1;
  animation: mdc-circular-progress-container-rotate calc(1568.2352941176ms * var(--mat-progress-spinner-animation-multiplier)) linear infinite;
}

.mdc-circular-progress__determinate-circle-graphic,
.mdc-circular-progress__indeterminate-circle-graphic {
  fill: transparent;
}

.mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
.mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
  stroke: var(--mat-progress-spinner-active-indicator-color, var(--mat-sys-primary));
}
@media (forced-colors: active) {
  .mat-mdc-progress-spinner .mdc-circular-progress__determinate-circle,
  .mat-mdc-progress-spinner .mdc-circular-progress__indeterminate-circle-graphic {
    stroke: CanvasText;
  }
}

.mdc-circular-progress__determinate-circle {
  transition: stroke-dashoffset 500ms cubic-bezier(0, 0, 0.2, 1);
}

.mdc-circular-progress__gap-patch {
  position: absolute;
  top: 0;
  left: 47.5%;
  box-sizing: border-box;
  width: 5%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress__gap-patch .mdc-circular-progress__indeterminate-circle-graphic {
  left: -900%;
  width: 2000%;
  transform: rotate(180deg);
}
.mdc-circular-progress__circle-clipper .mdc-circular-progress__indeterminate-circle-graphic {
  width: 200%;
}
.mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  left: -100%;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-left .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-left-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}
.mdc-circular-progress--indeterminate .mdc-circular-progress__circle-right .mdc-circular-progress__indeterminate-circle-graphic {
  animation: mdc-circular-progress-right-spin calc(1333ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

.mdc-circular-progress__circle-clipper {
  display: inline-flex;
  position: relative;
  width: 50%;
  height: 100%;
  overflow: hidden;
}

.mdc-circular-progress--indeterminate .mdc-circular-progress__spinner-layer {
  animation: mdc-circular-progress-spinner-layer-rotate calc(5332ms * var(--mat-progress-spinner-animation-multiplier)) cubic-bezier(0.4, 0, 0.2, 1) infinite both;
}

@keyframes mdc-circular-progress-container-rotate {
  to {
    transform: rotate(360deg);
  }
}
@keyframes mdc-circular-progress-spinner-layer-rotate {
  12.5% {
    transform: rotate(135deg);
  }
  25% {
    transform: rotate(270deg);
  }
  37.5% {
    transform: rotate(405deg);
  }
  50% {
    transform: rotate(540deg);
  }
  62.5% {
    transform: rotate(675deg);
  }
  75% {
    transform: rotate(810deg);
  }
  87.5% {
    transform: rotate(945deg);
  }
  100% {
    transform: rotate(1080deg);
  }
}
@keyframes mdc-circular-progress-left-spin {
  from {
    transform: rotate(265deg);
  }
  50% {
    transform: rotate(130deg);
  }
  to {
    transform: rotate(265deg);
  }
}
@keyframes mdc-circular-progress-right-spin {
  from {
    transform: rotate(-265deg);
  }
  50% {
    transform: rotate(-130deg);
  }
  to {
    transform: rotate(-265deg);
  }
}
`],encapsulation:2,changeDetection:0})}return n})();var BP=()=>[5];function VP(n,t){n&1&&(_(0,"mat-header-cell"),E(1," No "),g())}function jP(n,t){if(n&1&&(_(0,"mat-cell")(1,"span",24),E(2,"\u2116"),g(),E(3),g()),n&2){let e=t.$implicit;y(3),Ye(" ",e.id," ")}}function zP(n,t){n&1&&(_(0,"mat-header-cell")(1,"span",25),E(2,"\u0412\u043E\u043F\u0440\u043E\u0441"),g()())}function HP(n,t){if(n&1&&(_(0,"span",28),E(1),g()),n&2){let e=H().$implicit;y(),It(e.text)}}function UP(n,t){if(n&1){let e=Je();_(0,"mat-form-field",29)(1,"mat-label"),E(2,"\u0417\u0430\u043F\u043E\u043B\u043D\u0438 \u0441\u0432\u043E\u0439 \u0432\u0430\u0440\u0438\u0430\u043D\u0442:"),g(),_(3,"input",30,1),V("change",function(){He(e);let r=dt(4),o=H().$implicit,s=H();return Ue(s.onInputChange(r.value,o))}),g()()}if(n&2){let e=H().$implicit;y(3),R("value",e.text)}}function $P(n,t){if(n&1&&(_(0,"mat-cell")(1,"div",24),E(2,"\u0414\u0435\u0439\u0441\u0442\u0432\u0438\u0435:"),g(),fe(3,HP,2,1,"span",26)(4,UP,5,1,"mat-form-field",27),g()),n&2){let e=t.$implicit;y(3),R("ngIf",!e.isOwn),y(),R("ngIf",e.isOwn)}}function GP(n,t){n&1&&(_(0,"mat-header-cell"),E(1," \u041E\u0442\u0432\u0435\u0442 "),g())}function WP(n,t){if(n&1){let e=Je();_(0,"mat-button-toggle-group",32),V("change",function(r){He(e);let o=H().$implicit,s=H();return Ue(s.onValChange(r.value,o))}),_(1,"mat-button-toggle",33),E(2,"0"),g(),_(3,"mat-button-toggle",33),E(4,"1"),g(),_(5,"mat-button-toggle",33),E(6,"2"),g(),_(7,"mat-button-toggle",33),E(8,"3"),g(),_(9,"mat-button-toggle",33),E(10,"4"),g(),_(11,"mat-button-toggle",33),E(12,"5"),g()()}if(n&2){let e=H().$implicit;R("value",e.estimate),y(),R("value",0),y(2),R("value",1),y(2),R("value",2),y(2),R("value",3),y(2),R("value",4),y(2),R("value",5)}}function qP(n,t){if(n&1){let e=Je();_(0,"mat-form-field",29),Z(1,"mat-label"),_(2,"input",30,1),V("change",function(){He(e);let r=dt(3),o=H().$implicit,s=H();return Ue(s.onInputNotToggleChange(r.value,o))}),g()()}if(n&2){let e=H().$implicit;y(2),R("value",e.answer)}}function QP(n,t){if(n&1&&(_(0,"mat-cell")(1,"span",24),E(2,"\u0411\u0430\u043B\u044B:"),g(),fe(3,WP,13,7,"mat-button-toggle-group",31)(4,qP,4,1,"mat-form-field",27),g()),n&2){let e=t.$implicit;y(3),R("ngIf",e.id>0),y(),R("ngIf",e.id<0)}}function KP(n,t){n&1&&(_(0,"mat-header-cell"),E(1," \u041A\u043E\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439 "),Z(2,"br"),E(3," (\u043F\u043E \u0436\u0435\u043B\u0430\u043D\u0438\u044E)"),g())}function YP(n,t){if(n&1){let e=Je();_(0,"mat-cell")(1,"span",24),E(2,"\u041A\u043E\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439 "),Z(3,"br"),E(4," (\u043F\u043E \u0436\u0435\u043B\u0430\u043D\u0438\u044E)"),g(),_(5,"button",34),V("click",function(){let r=He(e).$implicit,o=H();return Ue(o.goToComment(r))}),E(6),g()()}if(n&2){let e=t.$implicit;y(6),Ye(" ",e.comment?"\u0418\u0441\u043F\u0440\u0430\u0432\u0438\u0442\u044C \u043A\u043E\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439":"\u0414\u043E\u0431\u0430\u0432\u0438\u0442\u044C \u043A\u043E\u043C\u0435\u043D\u0442\u0430\u0440\u0438\u0439"," ")}}function ZP(n,t){n&1&&Z(0,"mat-header-row")}function XP(n,t){n&1&&Z(0,"mat-row")}function JP(n,t){n&1&&Z(0,"mat-spinner",35)}function eL(n,t){n&1&&(_(0,"span"),E(1,"\u041E\u0442\u043F\u0440\u0430\u0432\u0438\u0442\u044C \u0430\u043D\u043A\u0435\u0442\u0443"),g())}var uk=(()=>{class n{constructor(e,i,r,o,s,a){this.dialog=e,this.snackBar=i,this.httpClient=r,this.questionsService=o,this.breakpointObserver=s,this.cdr=a,this.pageToShowEvent=new z,this.isLoad=!1,this.displayedColumns=["number","name","estimate","comment"]}ngAfterViewInit(){}ngOnInit(){this.breakpointObserver.observe(["(max-width: 768px)"]).subscribe(e=>{this.isMobile=e.matches}),this.questionsService.questionsReplaySubject.subscribe(e=>{console.log(e),this.questions=e,setTimeout(()=>{this.dataSource=new hm(e),this.dataSource.paginator=this.paginator},0)})}sendResult(){let e=this.questions.filter(o=>o.isOwn===!1),i=this.questions.filter(o=>o.isOwn),r=e.every(o=>o.estimate||o.answer)&&i.every(o=>o.text);{this.questionsService.questionsReplaySubject.next(this.questions);let o=JSON.stringify(this.questions);this.isLoad=!0,this.httpClient.post("https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net",o).subscribe(s=>{this.isLoad=!1,s&&(this.snackBar.open("\u0410\u043D\u043A\u0435\u0442\u0443 \u043F\u043E\u043B\u0443\u0447\u0438\u043B\u0430","\u0423\u0441\u043F\u0435\u0448\u043D\u043E!",{duration:15e3}),this.pageToShowEvent.emit(3))})}}onValChange(e,i){i.estimate=e}openSnackBar(){let i="\u041E\u0441\u0442\u0430\u043B\u0438\u0441\u044C \u0432\u043E\u043F\u0440\u043E\u0441\u044B: "+this.questions.filter(o=>!o.isOwn&&!o.estimate&&!o.answer||o.isOwn&&o.text==="").map(o=>o.id);this.snackBar.open(i,"Attention",{duration:3e3})}goToComment(e){this.openDialog(e)}openDialog(e){let i=this.dialog.open(XE,{disableClose:!0,data:e.comment});i.afterClosed().subscribe(r=>{(r||r==="")&&(e.comment=r,this.cdr.detectChanges()),i=null})}goToPage1(){this.questionsService.questionsReplaySubject.next(this.questions),this.pageToShowEvent.emit(1)}onInputChange(e,i){i.text=e}onInputNotToggleChange(e,i){i.answer=e}static{this.\u0275fac=function(i){return new(i||n)(q(J_),q(ek),q(ln),q(Gi),q(hi),q(Ae))}}static{this.\u0275cmp=A({type:n,selectors:[["app-page2"]],viewQuery:function(i,r){if(i&1&&Te(fl,5),i&2){let o;G(o=W())&&(r.paginator=o.first)}},outputs:{pageToShowEvent:"pageToShowEvent"},standalone:!1,decls:32,vars:11,consts:[["table",""],["answerInput",""],[1,"mainPage2"],[1,"leftPage2"],[1,"go-back-wrapper"],["mat-stroked-button","",1,"go-back-button",3,"click"],[1,"go-back-div"],[1,"go-back-text"],["id","girl","src","assets/second.jpg"],[1,"rightPage2"],[1,"mat-elevation-z8",3,"dataSource"],["matColumnDef","number"],[4,"matHeaderCellDef"],[4,"matCellDef"],["matColumnDef","name"],["matColumnDef","estimate"],["matColumnDef","comment"],[4,"matHeaderRowDef"],[4,"matRowDef","matRowDefColumns"],["showFirstLastButtons","",3,"pageSizeOptions","hidePageSize"],[1,"send-result"],["mat-raised-button","","color","accent","id","continue-button",3,"click","disabled"],["diameter","20",4,"ngIf"],[4,"ngIf"],[1,"mobile-label"],[1,"header-center"],["class","question-text",4,"ngIf"],["appearance","outline","class","no-padding",4,"ngIf"],[1,"question-text"],["appearance","outline",1,"no-padding"],["matInput","","autocomplete","off",3,"change","value"],["name","fontStyle","aria-label","Font Style",3,"value","change",4,"ngIf"],["name","fontStyle","aria-label","Font Style",3,"change","value"],[3,"value"],["mat-stroked-button","","color","primary",3,"click"],["diameter","20"]],template:function(i,r){i&1&&(_(0,"div",2)(1,"div",3)(2,"div",4)(3,"button",5),V("click",function(){return r.goToPage1()}),_(4,"div",6)(5,"mat-icon"),E(6,"arrow_back_ios"),g(),_(7,"span",7),E(8,"\u041D\u0430\u0437\u0430\u0434"),g()()()(),Z(9,"img",8),g(),_(10,"div",9)(11,"mat-table",10,0),wt(13,11),fe(14,VP,2,0,"mat-header-cell",12)(15,jP,4,1,"mat-cell",13),Ct(),wt(16,14),fe(17,zP,3,0,"mat-header-cell",12)(18,$P,5,2,"mat-cell",13),Ct(),wt(19,15),fe(20,GP,2,0,"mat-header-cell",12)(21,QP,5,2,"mat-cell",13),Ct(),wt(22,16),fe(23,KP,4,0,"mat-header-cell",12)(24,YP,7,1,"mat-cell",13),Ct(),fe(25,ZP,1,0,"mat-header-row",17)(26,XP,1,0,"mat-row",18),g(),Z(27,"mat-paginator",19),g()(),_(28,"div",20)(29,"button",21),V("click",function(){return r.sendResult()}),fe(30,JP,1,0,"mat-spinner",22)(31,eL,2,0,"span",23),g()()),i&2&&(y(11),j("isMobile",r.isMobile),R("dataSource",r.dataSource),y(14),R("matHeaderRowDef",r.displayedColumns),y(),R("matRowDefColumns",r.displayedColumns),y(),R("pageSizeOptions",zp(10,BP))("hidePageSize",!0),y(2),R("disabled",r.isLoad),y(),R("ngIf",r.isLoad),y(),R("ngIf",!r.isLoad))},dependencies:[_a,Gn,Wn,pi,ak,us,fl,fm,gm,ym,_m,pm,wm,vm,bm,Cm,Dm,nv,wl,Im],styles:[`.send-result{width:100%;display:flex;align-items:center;justify-content:center}.mat-paginator{margin-top:22px}.mat-paginator-range-label{margin:0!important;width:80px!important}.mat-header-cell{justify-content:center}.mat-cell{padding-right:3px;padding-left:3px;align-items:center}.mat-table{overflow:auto;height:470px;max-height:500px}@media(max-width:600px){.mainPage2{display:flex;flex-wrap:wrap;flex-direction:column;justify-content:center}#girl{margin-left:20px;width:80%;height:75%;object-fit:contain}.leftPage2{width:100%;display:flex;flex-direction:column}.rightPage2{margin-top:30px;width:100%;display:flex;flex-direction:column;align-items:center;justify-content:center}.mobile-label{width:100px!important;display:inline-block;font-weight:700}.mat-header-row{display:none}.mat-row{flex-direction:column;align-items:start;padding:8px 24px}}@media screen and (min-width:600px){.mat-form-field{width:95%;padding-bottom:0}.mainPage2{display:flex;flex-wrap:wrap;flex-direction:row;justify-content:center}#girl{margin-left:20px;width:80%;height:75%;object-fit:contain}.leftPage2{width:25%;display:flex;flex-direction:column}.rightPage2{margin-top:30px;width:75%;display:flex;flex-direction:column;align-items:center;justify-content:center}.mobile-hide{display:none!important}mat-table{width:100%}.mat-column-number{flex:0 0 3%}.mat-column-name{flex:0 0 43%;min-width:250px}.mat-column-estimate{flex:0 0 25%}}.question-text{margin-left:5px}.go-back-wrapper{display:flex;flex-direction:row;margin-bottom:20px}.go-back-button{height:48px;border:0}.go-back-div{display:flex;flex-direction:row;align-items:center;justify-content:center}.go-back-text{font-family:Roboto;font-style:normal;font-weight:400;font-size:14px;line-height:20px;color:#737373}mat-button-toggle{min-width:35px;width:35px;padding:0;text-align:center}.invisible-digit{visibility:hidden}.check-mark{position:absolute;left:50%;transform:translate(-50%)}:host ::ng-deep .mat-button-toggle-checkbox-wrapper{display:none!important}.mobile-label{display:none}mat-table{width:100%}mat-table.isMobile{width:100%;box-shadow:none}.isMobile mat-header-row{display:none}.isMobile mat-row{display:flex;flex-direction:column;align-items:stretch;height:auto;padding:8px 12px;margin-bottom:12px;border:1px solid #ddd;border-radius:8px;box-sizing:border-box}.isMobile mat-cell{display:flex;flex-direction:column;align-items:stretch;width:100%;min-height:auto;padding:10px 0;border-bottom:none;box-sizing:border-box}.isMobile .mobile-label{display:block;margin-bottom:6px;font-size:12px;font-weight:500;color:#666}.isMobile .question-text{white-space:normal;overflow-wrap:anywhere}.isMobile mat-form-field{width:100%}.isMobile mat-button-toggle-group{display:flex;width:100%}.isMobile mat-button-toggle{flex:1 1 0;min-width:0}.isMobile .buttons-container,.isMobile button{max-width:100%}@media(max-width:480px){.isMobile mat-row{padding:6px 10px}.isMobile mat-cell{padding:8px 0}.isMobile mat-button-toggle{font-size:12px}}
`],encapsulation:2})}}return n})();var mk=(()=>{class n{constructor(){this.pageToShowEvent=new z}ngOnInit(){}static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275cmp=A({type:n,selectors:[["app-page-end"]],outputs:{pageToShowEvent:"pageToShowEvent"},standalone:!1,decls:1,vars:0,consts:[["id","end","src","assets/end.jpg"]],template:function(i,r){i&1&&Z(0,"img",0)},styles:["body[_ngcontent-%COMP%]{margin:0}#end[_ngcontent-%COMP%]{height:100%;width:100%}"]})}}return n})();function iL(n,t){n&1&&(_(0,"th",13),E(1,"\u041A\u043E\u043C\u0435\u043D\u0442\u0430\u0440"),g())}function rL(n,t){if(n&1&&(_(0,"td",14)(1,"span",15),E(2,"\u041A\u043E\u043C\u0435\u043D\u0442\u0430\u0440"),g(),_(3,"span",16),E(4),g()()),n&2){let e=t.$implicit;y(4),It(e.comment||"\u2014")}}function oL(n,t){n&1&&(_(0,"th",13),E(1,"\u041E\u0446\u0456\u043D\u043A\u0430"),g())}function sL(n,t){if(n&1&&(_(0,"td",14)(1,"span",15),E(2,"\u041E\u0446\u0456\u043D\u043A\u0430"),g(),_(3,"span",17),E(4),g()()),n&2){let e=t.$implicit;y(4),Ye(" ",e.estimate??"\u2014"," ")}}function aL(n,t){n&1&&(_(0,"th",13),E(1,"\u041F\u0438\u0442\u0430\u043D\u043D\u044F"),g())}function lL(n,t){if(n&1&&(_(0,"td",14)(1,"span",15),E(2,"\u041F\u0438\u0442\u0430\u043D\u043D\u044F"),g(),_(3,"span",18),E(4),g()()),n&2){let e=t.$implicit;y(4),Ye(" ",e.text," ")}}function cL(n,t){n&1&&(_(0,"th",13),E(1,"\u041E\u0446\u0456\u043D\u043A\u0430 \u0430\u0432\u0442\u043E\u0440\u0430"),g())}function dL(n,t){if(n&1&&(_(0,"td",14)(1,"span",15),E(2,"\u041E\u0446\u0456\u043D\u043A\u0430 \u0430\u0432\u0442\u043E\u0440\u0430"),g(),_(3,"span",17),E(4),g()()),n&2){let e=t.$implicit;y(4),Ye(" ",e.estimateAuthor??"\u2014"," ")}}function uL(n,t){n&1&&(_(0,"th",13),E(1,"\u041A\u043E\u043C\u0435\u043D\u0442\u0430\u0440 \u0430\u0432\u0442\u043E\u0440\u0430"),g())}function mL(n,t){if(n&1&&(_(0,"td",14)(1,"span",15),E(2,"\u041A\u043E\u043C\u0435\u043D\u0442\u0430\u0440 \u0430\u0432\u0442\u043E\u0440\u0430"),g(),_(3,"span",16),E(4),g()()),n&2){let e=t.$implicit;y(4),Ye(" ",e.commentAuthor||"\u2014"," ")}}function hL(n,t){n&1&&Z(0,"tr",19)}function fL(n,t){n&1&&Z(0,"tr",20)}var hk=(()=>{class n{constructor(e,i,r){this.httpClient=e,this.questionsService=i,this.breakpointObserver=r,this.pageToShowEvent=new z,this.authToken="76c030ab71daae5908437a13af8bec91",this.questions=[],this.displayedColumns=["comment","estimate","text","estimateAuthor","commentAuthor"],this.isMobile=!1}ngOnInit(){this.breakpointObserver.observe(["(max-width: 768px)"]).subscribe(e=>{this.isMobile=e.matches}),this.questionsService.questionsReplaySubject.subscribe(e=>{console.log(e),this.questions=e})}static{this.\u0275fac=function(i){return new(i||n)(q(ln),q(Gi),q(hi))}}static{this.\u0275cmp=A({type:n,selectors:[["app-page-result"]],outputs:{pageToShowEvent:"pageToShowEvent"},standalone:!1,decls:22,vars:3,consts:[[1,"results-container"],[1,"results-title"],[1,"table-scroll"],["mat-table","",1,"results-table",3,"dataSource"],["matColumnDef","comment"],["mat-header-cell","",4,"matHeaderCellDef"],["mat-cell","",4,"matCellDef"],["matColumnDef","estimate"],["matColumnDef","text"],["matColumnDef","estimateAuthor"],["matColumnDef","commentAuthor"],["mat-header-row","",4,"matHeaderRowDef"],["mat-row","",4,"matRowDef","matRowDefColumns"],["mat-header-cell",""],["mat-cell",""],[1,"mobile-label"],[1,"cell-content"],[1,"estimate-value"],[1,"cell-content","question-text"],["mat-header-row",""],["mat-row",""]],template:function(i,r){i&1&&(_(0,"div",0)(1,"h2",1),E(2,"\u0420\u0435\u0437\u0443\u043B\u044C\u0442\u0430\u0442\u0438"),g(),_(3,"div",2)(4,"table",3),wt(5,4),fe(6,iL,2,0,"th",5)(7,rL,5,1,"td",6),Ct(),wt(8,7),fe(9,oL,2,0,"th",5)(10,sL,5,1,"td",6),Ct(),wt(11,8),fe(12,aL,2,0,"th",5)(13,lL,5,1,"td",6),Ct(),wt(14,9),fe(15,cL,2,0,"th",5)(16,dL,5,1,"td",6),Ct(),wt(17,10),fe(18,uL,2,0,"th",5)(19,mL,5,1,"td",6),Ct(),fe(20,hL,1,0,"tr",11)(21,fL,1,0,"tr",12),g()()()),i&2&&(y(4),R("dataSource",r.questions),y(16),R("matHeaderRowDef",r.displayedColumns),y(),R("matRowDefColumns",r.displayedColumns))},dependencies:[fm,gm,ym,_m,pm,wm,vm,bm,Cm,Dm],styles:[".results-container[_ngcontent-%COMP%]{width:100%;max-width:1600px;margin:0 auto;padding:16px;box-sizing:border-box}.results-title[_ngcontent-%COMP%]{margin:0 0 20px;text-align:center;font-size:24px;font-weight:600}.table-scroll[_ngcontent-%COMP%]{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch}.results-table[_ngcontent-%COMP%]{width:100%;min-width:700px;table-layout:fixed}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%], .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{padding:12px 8px;text-align:center;vertical-align:middle;white-space:normal;overflow-wrap:anywhere;box-sizing:border-box}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{font-weight:600}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]:nth-child(1), .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]:nth-child(1), .results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]:nth-child(5), .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]:nth-child(5){width:22%}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]:nth-child(2), .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]:nth-child(2), .results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]:nth-child(4), .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]:nth-child(4){width:10%}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]:nth-child(3), .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]:nth-child(3){width:36%}.cell-content[_ngcontent-%COMP%]{display:block;text-align:center;line-height:1.4}.mobile-label[_ngcontent-%COMP%]{display:none!important}@media(max-width:768px){.results-container[_ngcontent-%COMP%]{padding:8px}.results-title[_ngcontent-%COMP%]{margin-bottom:12px;font-size:20px}.results-table[_ngcontent-%COMP%]{min-width:600px}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%], .results-table[_ngcontent-%COMP%]   td[_ngcontent-%COMP%]{padding:6px 4px;font-size:11px;line-height:1.3}.results-table[_ngcontent-%COMP%]   th[_ngcontent-%COMP%]{font-size:10px}.estimate-value[_ngcontent-%COMP%]{min-width:0;min-height:0;font-size:12px}}"]})}}return n})();function gL(n,t){n&1&&(_(0,"mat-error"),E(1,"\u041D\u0435\u043F\u0440\u0430\u0432\u0438\u043B\u044C\u043D\u044B\u0439 \u043F\u0430\u0440\u043E\u043B\u044C!"),g())}function _L(n,t){if(n&1){let e=Je();_(0,"form",2),V("submit",function(){He(e);let r=H();return Ue(r.goToPage1())}),_(1,"div",3)(2,"h1"),E(3,"\u041F\u0440\u0438\u0432\u0435\u0442\u0441\u0442\u0432\u0443\u044E \u0412\u0430\u0441, \u043A\u043E\u0440\u043E\u043B\u0435\u0432\u0430 \u041A\u0441\u0435\u043D\u0438\u044F"),g(),_(4,"h3"),E(5," \u0414\u043B\u044F \u0434\u043E\u0441\u0442\u0443\u043F\u0430 \u0432\u0432\u0435\u0434\u0438\u0442\u0435 \u043F\u0430\u0440\u043E\u043B\u044C:"),g(),_(6,"mat-form-field",4)(7,"mat-label"),E(8,"\u041F\u0430\u0440\u043E\u043B\u044C:"),g(),Z(9,"input",5),fe(10,gL,2,0,"mat-error",6),g(),_(11,"button",7),E(12," \u0412\u0445\u043E\u0434 "),g(),Z(13,"img",8),g()()}if(n&2){let e=H();R("formGroup",e.startForm),y(10),R("ngIf",e.startForm.get("password").value!==e.rightPassword)}}function vL(n,t){if(n&1){let e=Je();_(0,"app-page1",9),V("pageToShowEvent",function(r){He(e);let o=H();return Ue(o.pageEventHandler(r))}),g()}}function bL(n,t){if(n&1){let e=Je();_(0,"app-page2",9),V("pageToShowEvent",function(r){He(e);let o=H();return Ue(o.pageEventHandler(r))}),g()}}function yL(n,t){if(n&1){let e=Je();_(0,"app-page-result",9),V("pageToShowEvent",function(r){He(e);let o=H();return Ue(o.pageEventHandler(r))}),g()}}function wL(n,t){if(n&1){let e=Je();_(0,"app-page-end",9),V("pageToShowEvent",function(r){He(e);let o=H();return Ue(o.pageEventHandler(r))}),g()}}var fk=(()=>{class n{constructor(e,i,r){this.httpClient=e,this.questionsService=i,this.formBuilder=r,this.title="\u0410\u043D\u043A\u0435\u0442\u0430 \u0440\u0430\u0431\u0430"}ngOnInit(){this.rightPassword=new Date().getDate()*777,this.pageToShow=0,this.startForm=this.formBuilder.group({password:["",this.passWordValidator]}),this.httpClient.get("assets/test.json",{responseType:"json"}).subscribe(e=>{this.questions=[];for(let r=0;r<e.length;r++){let o=new Wa;o.id=r+1,o.category=e[r].category,o.text=e[r].name,o.estimateAuthor=e[r].estimateAuthor,o.commentAuthor=e[r].commentAuthor,o.isOwn=!1,this.questions.push(o)}let i=this.questions.length+1;for(let r=i;r<i+3;r++){let o=new Wa;o.id=r,o.isOwn=!0,o.text="",this.questions.push(o)}}),this.httpClient.post("https://5b3943cfcda47d5c7885c5e06e3d8361.m.pipedream.net","\u042F \u0437\u0430\u0448\u043B\u0430 (\u041A\u0441\u0435\u043D\u0438\u044F)").subscribe(e=>{console.log("")})}pageEventHandler(e){this.pageToShow=e}ngOnDestroy(){}goToPage1(){this.questionsService.questionsReplaySubject.next(this.questions),this.startForm.get("password").value===this.rightPassword.toString()&&(this.pageToShow=1),this.startForm.get("password").value==="1311"&&(this.pageToShow=-1),this.pageToShow=1}passWordValidator(e){return e.value===(new Date().getDate()*777).toString()?{wrongPassword:!0}:null}static{this.\u0275fac=function(i){return new(i||n)(q(ln),q(Gi),q(cx))}}static{this.\u0275cmp=A({type:n,selectors:[["app-root"]],standalone:!1,decls:5,vars:5,consts:[[3,"formGroup","submit",4,"ngIf"],[3,"pageToShowEvent",4,"ngIf"],[3,"submit","formGroup"],[1,"mainDiv"],["appearance","outline","autocomplete","off"],["matInput","","formControlName","password","autocomplete","off"],[4,"ngIf"],["mat-raised-button","","color","accent","type","submit","id","continue-button"],["src","assets/2.jpg","alt","sign",2,"margin-top","20px","height","40%"],[3,"pageToShowEvent"]],template:function(i,r){i&1&&fe(0,_L,14,2,"form",0)(1,vL,1,0,"app-page1",1)(2,bL,1,0,"app-page2",1)(3,yL,1,0,"app-page-result",1)(4,wL,1,0,"app-page-end",1),i&2&&(R("ngIf",r.pageToShow===0),y(),R("ngIf",r.pageToShow===1),y(),R("ngIf",r.pageToShow===2),y(),R("ngIf",r.pageToShow===3),y(),R("ngIf",r.pageToShow===4))},dependencies:[_a,sx,es,Tu,ZD,$r,t_,Gn,Wn,pi,k_,us,dE,uk,mk,hk],styles:[".mainDiv[_ngcontent-%COMP%]{display:flex;flex-direction:column;align-items:center;justify-content:center}"]})}}return n})();var iv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[qr,oe]})}return n})();var rv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe]})}return n})();var CL=["mat-internal-form-field",""],DL=["*"],pk=(()=>{class n{labelPosition="after";static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["div","mat-internal-form-field",""]],hostAttrs:[1,"mdc-form-field","mat-internal-form-field"],hostVars:2,hostBindings:function(i,r){i&2&&j("mdc-form-field--align-end",r.labelPosition==="before")},inputs:{labelPosition:"labelPosition"},attrs:CL,ngContentSelectors:DL,decls:1,vars:0,template:function(i,r){i&1&&(Ee(),ie(0))},styles:[`.mat-internal-form-field {
  -moz-osx-font-smoothing: grayscale;
  -webkit-font-smoothing: antialiased;
  display: inline-flex;
  align-items: center;
  vertical-align: middle;
}
.mat-internal-form-field > label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
  order: 0;
}
[dir=rtl] .mat-internal-form-field > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
}

.mdc-form-field--align-end > label {
  margin-left: auto;
  margin-right: 0;
  padding-left: 0;
  padding-right: 4px;
  order: -1;
}
[dir=rtl] .mdc-form-field--align-end .mdc-form-field--align-end label {
  margin-left: 0;
  margin-right: auto;
  padding-left: 4px;
  padding-right: 0;
}
`],encapsulation:2,changeDetection:0})}return n})();var xL=["input"],EL=["label"],kL=["*"],ov={color:"accent",clickAction:"check-indeterminate",disabledInteractive:!1},SL=new v("mat-checkbox-default-options",{providedIn:"root",factory:()=>ov}),Dt=(function(n){return n[n.Init=0]="Init",n[n.Checked=1]="Checked",n[n.Unchecked=2]="Unchecked",n[n.Indeterminate=3]="Indeterminate",n})(Dt||{}),sv=class{source;checked},IL=(()=>{class n{_elementRef=u(P);_changeDetectorRef=u(Ae);_ngZone=u(F);_animationsDisabled=Fe();_options=u(SL,{optional:!0});focus(){this._inputElement.nativeElement.focus()}_createChangeEvent(e){let i=new sv;return i.source=this,i.checked=e,i}_getAnimationTargetElement(){return this._inputElement?.nativeElement}_animationClasses={uncheckedToChecked:"mdc-checkbox--anim-unchecked-checked",uncheckedToIndeterminate:"mdc-checkbox--anim-unchecked-indeterminate",checkedToUnchecked:"mdc-checkbox--anim-checked-unchecked",checkedToIndeterminate:"mdc-checkbox--anim-checked-indeterminate",indeterminateToChecked:"mdc-checkbox--anim-indeterminate-checked",indeterminateToUnchecked:"mdc-checkbox--anim-indeterminate-unchecked"};ariaLabel="";ariaLabelledby=null;ariaDescribedby;ariaExpanded;ariaControls;ariaOwns;_uniqueId;id;get inputId(){return`${this.id||this._uniqueId}-input`}required=!1;labelPosition="after";name=null;change=new z;indeterminateChange=new z;value;disableRipple=!1;_inputElement;_labelElement;tabIndex;color;disabledInteractive;_onTouched=()=>{};_currentAnimationClass="";_currentCheckState=Dt.Init;_controlValueAccessorChangeFn=()=>{};_validatorChangeFn=()=>{};constructor(){u(it).load(qi);let e=u(new an("tabindex"),{optional:!0});this._options=this._options||ov,this.color=this._options.color||ov.color,this.tabIndex=e==null?0:parseInt(e)||0,this.id=this._uniqueId=u(Ie).getId("mat-mdc-checkbox-"),this.disabledInteractive=this._options?.disabledInteractive??!1}ngOnChanges(e){e.required&&this._validatorChangeFn()}ngAfterViewInit(){this._syncIndeterminate(this.indeterminate)}get checked(){return this._checked}set checked(e){e!=this.checked&&(this._checked=e,this._changeDetectorRef.markForCheck())}_checked=!1;get disabled(){return this._disabled}set disabled(e){e!==this.disabled&&(this._disabled=e,this._changeDetectorRef.markForCheck())}_disabled=!1;get indeterminate(){return this._indeterminate()}set indeterminate(e){let i=e!=this._indeterminate();this._indeterminate.set(e),i&&(e?this._transitionCheckState(Dt.Indeterminate):this._transitionCheckState(this.checked?Dt.Checked:Dt.Unchecked),this.indeterminateChange.emit(e)),this._syncIndeterminate(e)}_indeterminate=Oe(!1);_isRippleDisabled(){return this.disableRipple||this.disabled}_onLabelTextChange(){this._changeDetectorRef.detectChanges()}writeValue(e){this.checked=!!e}registerOnChange(e){this._controlValueAccessorChangeFn=e}registerOnTouched(e){this._onTouched=e}setDisabledState(e){this.disabled=e}validate(e){return this.required&&e.value!==!0?{required:!0}:null}registerOnValidatorChange(e){this._validatorChangeFn=e}_transitionCheckState(e){let i=this._currentCheckState,r=this._getAnimationTargetElement();if(!(i===e||!r)&&(this._currentAnimationClass&&r.classList.remove(this._currentAnimationClass),this._currentAnimationClass=this._getAnimationClassForCheckStateTransition(i,e),this._currentCheckState=e,this._currentAnimationClass.length>0)){r.classList.add(this._currentAnimationClass);let o=this._currentAnimationClass;this._ngZone.runOutsideAngular(()=>{setTimeout(()=>{r.classList.remove(o)},1e3)})}}_emitChangeEvent(){this._controlValueAccessorChangeFn(this.checked),this.change.emit(this._createChangeEvent(this.checked)),this._inputElement&&(this._inputElement.nativeElement.checked=this.checked)}toggle(){this.checked=!this.checked,this._controlValueAccessorChangeFn(this.checked)}_handleInputClick(){let e=this._options?.clickAction;!this.disabled&&e!=="noop"?(this.indeterminate&&e!=="check"&&Promise.resolve().then(()=>{this._indeterminate.set(!1),this.indeterminateChange.emit(!1)}),this._checked=!this._checked,this._transitionCheckState(this._checked?Dt.Checked:Dt.Unchecked),this._emitChangeEvent()):(this.disabled&&this.disabledInteractive||!this.disabled&&e==="noop")&&(this._inputElement.nativeElement.checked=this.checked,this._inputElement.nativeElement.indeterminate=this.indeterminate)}_onInteractionEvent(e){e.stopPropagation()}_onBlur(){Promise.resolve().then(()=>{this._onTouched(),this._changeDetectorRef.markForCheck()})}_getAnimationClassForCheckStateTransition(e,i){if(this._animationsDisabled)return"";switch(e){case Dt.Init:if(i===Dt.Checked)return this._animationClasses.uncheckedToChecked;if(i==Dt.Indeterminate)return this._checked?this._animationClasses.checkedToIndeterminate:this._animationClasses.uncheckedToIndeterminate;break;case Dt.Unchecked:return i===Dt.Checked?this._animationClasses.uncheckedToChecked:this._animationClasses.uncheckedToIndeterminate;case Dt.Checked:return i===Dt.Unchecked?this._animationClasses.checkedToUnchecked:this._animationClasses.checkedToIndeterminate;case Dt.Indeterminate:return i===Dt.Checked?this._animationClasses.indeterminateToChecked:this._animationClasses.indeterminateToUnchecked}return""}_syncIndeterminate(e){let i=this._inputElement;i&&(i.nativeElement.indeterminate=e)}_onInputClick(){this._handleInputClick()}_onTouchTargetClick(){this._handleInputClick(),this.disabled||this._inputElement.nativeElement.focus()}_preventBubblingFromLabel(e){e.target&&this._labelElement.nativeElement.contains(e.target)&&e.stopPropagation()}static \u0275fac=function(i){return new(i||n)};static \u0275cmp=A({type:n,selectors:[["mat-checkbox"]],viewQuery:function(i,r){if(i&1&&Te(xL,5)(EL,5),i&2){let o;G(o=W())&&(r._inputElement=o.first),G(o=W())&&(r._labelElement=o.first)}},hostAttrs:[1,"mat-mdc-checkbox"],hostVars:16,hostBindings:function(i,r){i&2&&(Nt("id",r.id),J("tabindex",null)("aria-label",null)("aria-labelledby",null),St(r.color?"mat-"+r.color:"mat-accent"),j("_mat-animation-noopable",r._animationsDisabled)("mdc-checkbox--disabled",r.disabled)("mat-mdc-checkbox-disabled",r.disabled)("mat-mdc-checkbox-checked",r.checked)("mat-mdc-checkbox-disabled-interactive",r.disabledInteractive))},inputs:{ariaLabel:[0,"aria-label","ariaLabel"],ariaLabelledby:[0,"aria-labelledby","ariaLabelledby"],ariaDescribedby:[0,"aria-describedby","ariaDescribedby"],ariaExpanded:[2,"aria-expanded","ariaExpanded",N],ariaControls:[0,"aria-controls","ariaControls"],ariaOwns:[0,"aria-owns","ariaOwns"],id:"id",required:[2,"required","required",N],labelPosition:"labelPosition",name:"name",value:"value",disableRipple:[2,"disableRipple","disableRipple",N],tabIndex:[2,"tabIndex","tabIndex",e=>e==null?void 0:ht(e)],color:"color",disabledInteractive:[2,"disabledInteractive","disabledInteractive",N],checked:[2,"checked","checked",N],disabled:[2,"disabled","disabled",N],indeterminate:[2,"indeterminate","indeterminate",N]},outputs:{change:"change",indeterminateChange:"indeterminateChange"},exportAs:["matCheckbox"],features:[ve([{provide:Ur,useExisting:bt(()=>n),multi:!0},{provide:ts,useExisting:n,multi:!0}]),Le],ngContentSelectors:kL,decls:15,vars:23,consts:[["checkbox",""],["input",""],["label",""],["mat-internal-form-field","",3,"click","labelPosition"],[1,"mdc-checkbox"],["aria-hidden","true",1,"mat-mdc-checkbox-touch-target",3,"click"],["type","checkbox",1,"mdc-checkbox__native-control",3,"blur","click","change","checked","indeterminate","disabled","id","required","tabIndex"],["aria-hidden","true",1,"mdc-checkbox__ripple"],["aria-hidden","true",1,"mdc-checkbox__background"],["focusable","false","viewBox","0 0 24 24",1,"mdc-checkbox__checkmark"],["fill","none","d","M1.73,12.91 8.1,19.28 22.79,4.59",1,"mdc-checkbox__checkmark-path"],[1,"mdc-checkbox__mixedmark"],["mat-ripple","","aria-hidden","true",1,"mat-mdc-checkbox-ripple","mat-focus-indicator",3,"matRippleTrigger","matRippleDisabled","matRippleCentered"],[1,"mdc-label",3,"for"]],template:function(i,r){if(i&1&&(Ee(),_(0,"div",3),V("click",function(s){return r._preventBubblingFromLabel(s)}),_(1,"div",4,0)(3,"div",5),V("click",function(){return r._onTouchTargetClick()}),g(),_(4,"input",6,1),V("blur",function(){return r._onBlur()})("click",function(){return r._onInputClick()})("change",function(s){return r._onInteractionEvent(s)}),g(),Z(6,"div",7),_(7,"div",8),yt(),_(8,"svg",9),Z(9,"path",10),g(),ni(),Z(10,"div",11),g(),Z(11,"div",12),g(),_(12,"label",13,2),ie(14),g()()),i&2){let o=dt(2);R("labelPosition",r.labelPosition),y(4),j("mdc-checkbox--selected",r.checked),R("checked",r.checked)("indeterminate",r.indeterminate)("disabled",r.disabled&&!r.disabledInteractive)("id",r.inputId)("required",r.required)("tabIndex",r.disabled&&!r.disabledInteractive?-1:r.tabIndex),J("aria-label",r.ariaLabel||null)("aria-labelledby",r.ariaLabelledby)("aria-describedby",r.ariaDescribedby)("aria-checked",r.indeterminate?"mixed":null)("aria-controls",r.ariaControls)("aria-disabled",r.disabled&&r.disabledInteractive?!0:null)("aria-expanded",r.ariaExpanded)("aria-owns",r.ariaOwns)("name",r.name)("value",r.value),y(7),R("matRippleTrigger",o)("matRippleDisabled",r.disableRipple||r.disabled)("matRippleCentered",!0),y(),R("for",r.inputId)}},dependencies:[ls,pk],styles:[`.mdc-checkbox {
  display: inline-block;
  position: relative;
  flex: 0 0 18px;
  box-sizing: content-box;
  width: 18px;
  height: 18px;
  line-height: 0;
  white-space: nowrap;
  cursor: pointer;
  vertical-align: bottom;
  padding: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  margin: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}
.mdc-checkbox:hover > .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:hover > .mat-mdc-checkbox-ripple > .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control:focus ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-focus-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-unselected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-unselected-pressed-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-hover-state-layer-opacity, var(--mat-sys-hover-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-hover-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-focus-state-layer-opacity, var(--mat-sys-focus-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox .mdc-checkbox__native-control:focus:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-focus-state-layer-color, var(--mat-sys-primary));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked + .mdc-checkbox__ripple {
  opacity: var(--mat-checkbox-selected-pressed-state-layer-opacity, var(--mat-sys-pressed-state-layer-opacity));
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox:active > .mdc-checkbox__native-control:checked ~ .mat-mdc-checkbox-ripple .mat-ripple-element {
  background-color: var(--mat-checkbox-selected-pressed-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control ~ .mat-mdc-checkbox-ripple .mat-ripple-element,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control + .mdc-checkbox__ripple {
  background-color: var(--mat-checkbox-unselected-hover-state-layer-color, var(--mat-sys-on-surface));
}
.mdc-checkbox .mdc-checkbox__native-control {
  position: absolute;
  margin: 0;
  padding: 0;
  opacity: 0;
  cursor: inherit;
  z-index: 1;
  width: var(--mat-checkbox-state-layer-size, 40px);
  height: var(--mat-checkbox-state-layer-size, 40px);
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  right: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - var(--mat-checkbox-state-layer-size, 40px)) / 2);
}

.mdc-checkbox--disabled {
  cursor: default;
  pointer-events: none;
}

.mdc-checkbox__background {
  display: inline-flex;
  position: absolute;
  align-items: center;
  justify-content: center;
  box-sizing: border-box;
  width: 18px;
  height: 18px;
  border: 2px solid currentColor;
  border-radius: 2px;
  background-color: transparent;
  pointer-events: none;
  will-change: background-color, border-color;
  transition: background-color 90ms cubic-bezier(0.4, 0, 0.6, 1), border-color 90ms cubic-bezier(0.4, 0, 0.6, 1);
  -webkit-print-color-adjust: exact;
  color-adjust: exact;
  border-color: var(--mat-checkbox-unselected-icon-color, var(--mat-sys-on-surface-variant));
  top: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
  left: calc((var(--mat-checkbox-state-layer-size, 40px) - 18px) / 2);
}

.mdc-checkbox__native-control:enabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:enabled:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}
@media (forced-colors: active) {
  .mdc-checkbox__native-control:disabled:checked ~ .mdc-checkbox__background,
  .mdc-checkbox__native-control:disabled:indeterminate ~ .mdc-checkbox__background {
    border-color: GrayText;
  }
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-hover-icon-color, var(--mat-sys-on-surface));
  background-color: transparent;
}

.mdc-checkbox:hover > .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox:hover > .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-hover-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox__native-control:focus:focus:not(:checked) ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:not(:indeterminate) ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-unselected-focus-icon-color, var(--mat-sys-on-surface));
}

.mdc-checkbox__native-control:focus:focus:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:focus:focus:indeterminate ~ .mdc-checkbox__background {
  border-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
  background-color: var(--mat-checkbox-selected-focus-icon-color, var(--mat-sys-primary));
}

.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
  border-color: var(--mat-checkbox-disabled-unselected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox:hover > .mdc-checkbox__native-control ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox .mdc-checkbox__native-control:focus ~ .mdc-checkbox__background,
  .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__background {
    border-color: GrayText;
  }
}
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  background-color: var(--mat-checkbox-disabled-selected-icon-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
  border-color: transparent;
}

.mdc-checkbox__checkmark {
  position: absolute;
  top: 0;
  right: 0;
  bottom: 0;
  left: 0;
  width: 100%;
  opacity: 0;
  transition: opacity 180ms cubic-bezier(0.4, 0, 0.6, 1);
  color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__checkmark {
    color: CanvasText;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
  color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__checkmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__checkmark {
    color: GrayText;
  }
}

.mdc-checkbox__checkmark-path {
  transition: stroke-dashoffset 180ms cubic-bezier(0.4, 0, 0.6, 1);
  stroke: currentColor;
  stroke-width: 3.12px;
  stroke-dashoffset: 29.7833385;
  stroke-dasharray: 29.7833385;
}

.mdc-checkbox__mixedmark {
  width: 100%;
  height: 0;
  transform: scaleX(0) rotate(0deg);
  border-width: 1px;
  border-style: solid;
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
  border-color: var(--mat-checkbox-selected-checkmark-color, var(--mat-sys-on-primary));
}
@media (forced-colors: active) {
  .mdc-checkbox__mixedmark {
    margin: 0 1px;
  }
}

.mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
  border-color: var(--mat-checkbox-disabled-selected-checkmark-color, var(--mat-sys-surface));
}
@media (forced-colors: active) {
  .mdc-checkbox--disabled .mdc-checkbox__mixedmark, .mdc-checkbox--disabled.mat-mdc-checkbox-disabled-interactive .mdc-checkbox__mixedmark {
    border-color: GrayText;
  }
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__background,
.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__background,
.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__background,
.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__background {
  animation-duration: 180ms;
  animation-timing-function: linear;
}

.mdc-checkbox--anim-unchecked-checked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-unchecked-checked-checkmark-path 180ms linear;
  transition: none;
}

.mdc-checkbox--anim-unchecked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-unchecked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-unchecked .mdc-checkbox__checkmark-path {
  animation: mdc-checkbox-checked-unchecked-checkmark-path 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__checkmark {
  animation: mdc-checkbox-checked-indeterminate-checkmark 90ms linear;
  transition: none;
}
.mdc-checkbox--anim-checked-indeterminate .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-checked-indeterminate-mixedmark 90ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__checkmark {
  animation: mdc-checkbox-indeterminate-checked-checkmark 500ms linear;
  transition: none;
}
.mdc-checkbox--anim-indeterminate-checked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-checked-mixedmark 500ms linear;
  transition: none;
}

.mdc-checkbox--anim-indeterminate-unchecked .mdc-checkbox__mixedmark {
  animation: mdc-checkbox-indeterminate-unchecked-mixedmark 300ms linear;
  transition: none;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background {
  transition: border-color 90ms cubic-bezier(0, 0, 0.2, 1), background-color 90ms cubic-bezier(0, 0, 0.2, 1);
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path {
  stroke-dashoffset: 0;
}

.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transition: opacity 180ms cubic-bezier(0, 0, 0.2, 1), transform 180ms cubic-bezier(0, 0, 0.2, 1);
  opacity: 1;
}
.mdc-checkbox__native-control:checked ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(-45deg);
}

.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__checkmark {
  transform: rotate(45deg);
  opacity: 0;
  transition: opacity 90ms cubic-bezier(0.4, 0, 0.6, 1), transform 90ms cubic-bezier(0.4, 0, 0.6, 1);
}
.mdc-checkbox__native-control:indeterminate ~ .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transform: scaleX(1) rotate(0deg);
  opacity: 1;
}

@keyframes mdc-checkbox-unchecked-checked-checkmark-path {
  0%, 50% {
    stroke-dashoffset: 29.7833385;
  }
  50% {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
  }
  100% {
    stroke-dashoffset: 0;
  }
}
@keyframes mdc-checkbox-unchecked-indeterminate-mixedmark {
  0%, 68.2% {
    transform: scaleX(0);
  }
  68.2% {
    animation-timing-function: cubic-bezier(0, 0, 0, 1);
  }
  100% {
    transform: scaleX(1);
  }
}
@keyframes mdc-checkbox-checked-unchecked-checkmark-path {
  from {
    animation-timing-function: cubic-bezier(0.4, 0, 1, 1);
    opacity: 1;
    stroke-dashoffset: 0;
  }
  to {
    opacity: 0;
    stroke-dashoffset: -29.7833385;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-checkmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(45deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-checkmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(45deg);
    opacity: 0;
  }
  to {
    transform: rotate(360deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-checked-indeterminate-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0, 0, 0.2, 1);
    transform: rotate(-45deg);
    opacity: 0;
  }
  to {
    transform: rotate(0deg);
    opacity: 1;
  }
}
@keyframes mdc-checkbox-indeterminate-checked-mixedmark {
  from {
    animation-timing-function: cubic-bezier(0.14, 0, 0, 1);
    transform: rotate(0deg);
    opacity: 1;
  }
  to {
    transform: rotate(315deg);
    opacity: 0;
  }
}
@keyframes mdc-checkbox-indeterminate-unchecked-mixedmark {
  0% {
    animation-timing-function: linear;
    transform: scaleX(1);
    opacity: 1;
  }
  32.8%, 100% {
    transform: scaleX(0);
    opacity: 0;
  }
}
.mat-mdc-checkbox {
  display: inline-block;
  position: relative;
  -webkit-tap-highlight-color: transparent;
}
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-touch-target,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__native-control,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__ripple,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mat-mdc-checkbox-ripple::before,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__checkmark > .mdc-checkbox__checkmark-path,
.mat-mdc-checkbox._mat-animation-noopable > .mat-internal-form-field > .mdc-checkbox > .mdc-checkbox__background > .mdc-checkbox__mixedmark {
  transition: none !important;
  animation: none !important;
}
.mat-mdc-checkbox label {
  cursor: pointer;
}
.mat-mdc-checkbox .mat-internal-form-field {
  color: var(--mat-checkbox-label-text-color, var(--mat-sys-on-surface));
  font-family: var(--mat-checkbox-label-text-font, var(--mat-sys-body-medium-font));
  line-height: var(--mat-checkbox-label-text-line-height, var(--mat-sys-body-medium-line-height));
  font-size: var(--mat-checkbox-label-text-size, var(--mat-sys-body-medium-size));
  letter-spacing: var(--mat-checkbox-label-text-tracking, var(--mat-sys-body-medium-tracking));
  font-weight: var(--mat-checkbox-label-text-weight, var(--mat-sys-body-medium-weight));
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive {
  pointer-events: auto;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled.mat-mdc-checkbox-disabled-interactive input {
  cursor: default;
}
.mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
  cursor: default;
  color: var(--mat-checkbox-disabled-label-color, color-mix(in srgb, var(--mat-sys-on-surface) 38%, transparent));
}
@media (forced-colors: active) {
  .mat-mdc-checkbox.mat-mdc-checkbox-disabled label {
    color: GrayText;
  }
}
.mat-mdc-checkbox label:empty {
  display: none;
}
.mat-mdc-checkbox .mdc-checkbox__ripple {
  opacity: 0;
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple,
.mdc-checkbox__ripple {
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  position: absolute;
  border-radius: 50%;
  pointer-events: none;
}
.mat-mdc-checkbox .mat-mdc-checkbox-ripple:not(:empty),
.mdc-checkbox__ripple:not(:empty) {
  transform: translateZ(0);
}

.mat-mdc-checkbox-ripple .mat-ripple-element {
  opacity: 0.1;
}

.mat-mdc-checkbox-touch-target {
  position: absolute;
  top: 50%;
  left: 50%;
  height: var(--mat-checkbox-touch-target-size, 48px);
  width: var(--mat-checkbox-touch-target-size, 48px);
  transform: translate(-50%, -50%);
  display: var(--mat-checkbox-touch-target-display, block);
}

.mat-mdc-checkbox .mat-mdc-checkbox-ripple::before {
  border-radius: 50%;
}

.mdc-checkbox__native-control:focus-visible ~ .mat-focus-indicator::before {
  content: "";
}
`],encapsulation:2,changeDetection:0})}return n})(),av=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[IL,oe]})}return n})();var ML=new v("mat-chips-default-options",{providedIn:"root",factory:()=>({separatorKeyCodes:[13]})});var lv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({providers:[cs,{provide:ML,useValue:{separatorKeyCodes:[13]}}],imports:[Qi,oe]})}return n})();var Cl=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe]})}return n})();var cv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[ss,Qi,am,oe,Cl]})}return n})();var dv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[qn,oe,qn]})}return n})();var uv=(()=>{class n{static \u0275fac=function(i){return new(i||n)};static \u0275mod=I({type:n});static \u0275inj=S({imports:[oe]})}return n})();var gk=(()=>{class n{static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=I({type:n})}static{this.\u0275inj=S({imports:[iv,Ki,rv,av,lv,Cl,Yi,yl,sl,cv,dv,uv,iv,Ki,rv,av,lv,Cl,Yi,yl,sl,cv,dv,uv]})}}return n})();var _k=(()=>{class n{static{this.\u0275fac=function(i){return new(i||n)}}static{this.\u0275mod=I({type:n,bootstrap:[fk]})}static{this.\u0275inj=S({imports:[Ea,AD,dx,ux,hg,gk,YE,FE,UE,yl,Ki,sl,wl,Im]})}}return n})();var vk={production:!0};vk.production&&void 0;sg().bootstrapModule(_k).catch(n=>console.error(n));
