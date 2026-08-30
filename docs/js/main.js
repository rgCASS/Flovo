(function(){
  const packs={zh:window.flovoI18nZh,en:window.flovoI18nEn};
  // Resolve a dot-delimited translation key from the selected language pack.
  const getValue=(obj,path)=>path.split('.').reduce((value,key)=>value&&value[key],obj);
  const requested=new URLSearchParams(window.location.search).get('lang');
  const browserLang=(navigator.language||'en').toLowerCase().startsWith('zh')?'zh':'en';
  let storedLang='';try{storedLang=localStorage.getItem('flovo-lang')||'';}catch(_error){}
  let lang=(requested==='zh'||requested==='en')?requested:(storedLang||browserLang);
  if(!packs[lang]) lang='en';
  // Persist language when storage is available; private browsing may reject writes.
  function safeStorageSet(value){try{localStorage.setItem('flovo-lang',value);}catch(_error){}}
  // Replace every marked node and update the document language and toggle state.
  function applyLanguage(next){
    lang=packs[next]?next:'en';document.documentElement.lang=lang;
    document.querySelectorAll('[data-i18n]').forEach(node=>{const value=getValue(packs[lang],node.dataset.i18n);if(typeof value==='string'){if(node.dataset.i18nHtml==='true')node.innerHTML=value;else node.textContent=value;}});
    document.querySelectorAll('[data-lang]').forEach(node=>node.classList.toggle('active',node.dataset.lang===lang));safeStorageSet(lang);
  }
  // Apply a dependency-free, intentionally small highlighter to JSON and Rust blocks.
  function highlight(code){
    const escaped=code.textContent.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    const isJson=code.classList.contains('language-json');
    const pattern=isJson?/("(?:\\.|[^"\\])*"(?=\s*:))|("(?:\\.|[^"\\])*")|(\b(?:true|false|null)\b)|(-?\b\d+(?:\.\d+)?\b)/g:/(\/\/[^\n]*)|("(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*')|(\b(?:fn|struct|impl|trait|async|let|self|use|pub|return)\b)/g;
    // Keep JSON token mapping intact; Rust keywords use the third capture group.
    code.innerHTML=escaped.replace(pattern,isJson
      ? (match,key,string,primitive)=>{if(key)return '<span class="token-key">'+match+'</span>';if(primitive)return '<span class="token-number">'+match+'</span>';return match;}
      : (match,comment,string,keyword)=>{if(comment)return '<span class="token-comment">'+match+'</span>';if(string)return '<span class="token-string">'+match+'</span>';if(keyword)return '<span class="token-key">'+match+'</span>';return match;});
  }
  document.addEventListener('DOMContentLoaded',()=>{
    applyLanguage(lang);document.querySelectorAll('code[class*="language-"]').forEach(highlight);
    document.querySelector('.lang-switch').addEventListener('click',()=>applyLanguage(lang==='en'?'zh':'en'));
    document.querySelector('.copy-button').addEventListener('click',async event=>{const button=event.currentTarget;try{await navigator.clipboard.writeText(document.querySelector('#workflow-code code').textContent);button.textContent='✓';setTimeout(()=>button.textContent='⧉',1200);}catch(_error){button.textContent='!';}});
    const links=[...document.querySelectorAll('.nav-links a')];const sections=links.map(link=>document.querySelector(link.getAttribute('href'))).filter(Boolean);const observer=new IntersectionObserver(entries=>entries.forEach(entry=>{if(entry.isIntersecting)links.forEach(link=>link.classList.toggle('active',link.getAttribute('href')==='#'+entry.target.id));}),{rootMargin:'-30% 0px -60%'});sections.forEach(section=>observer.observe(section));
  });
})();
