import { useEffect, useMemo, useState } from 'react'
import {
  Activity, AlertTriangle, CheckCircle2, Database, Eye, EyeOff,
  KeyRound, Link2, LoaderCircle, LockKeyhole, Power, PowerOff,
  RefreshCw, ShieldCheck, TestTube2, Trash2, WalletCards,
} from 'lucide-react'
import { API_BASE, ownerAccessToken } from './api'

type Mode = 'TESTNET'|'LIVE'
type AccountSummary = {
  mode:Mode;host:string;wallet_balance:number;available_balance:number;
  unrealized_pnl:number;active_positions:number;hedge_mode:boolean|null;
  clock_offset_ms:number;tested_at:string;orders_created:false
}
type Connection = {
  mode:Mode;label:string;host:string;configured:boolean;active:boolean;
  fingerprint:string|null;last_test_ok:boolean;last_test_at:string|null;
  last_error:string|null;account:AccountSummary|null;storage:string;secrets_returned:false
}
type ConnectionStatus = {
  version:string;
  vault:{ready:boolean;storage:string;reason:string|null;loaded_at:string|null};
  connections:Record<Mode,Connection>;
  safety:{https_required:boolean;secrets_returned_to_browser:false;connection_test_creates_orders:false;activation_arms_orders:false;live_orders_require_v25_gates:true;withdrawals_supported:false}
}
type FormState = {apiKey:string;secretKey:string;showApi:boolean;showSecret:boolean;accepted:boolean}

const emptyForm = ():FormState => ({apiKey:'',secretKey:'',showApi:false,showSecret:false,accepted:false})
const money = (value:number|undefined) => value == null ? '—' : value.toLocaleString('tr-TR',{minimumFractionDigits:2,maximumFractionDigits:4})
const date = (value:string|null|undefined) => value ? new Date(value).toLocaleString('tr-TR') : '—'

function errorText(value:unknown):string {
  if (typeof value === 'string') return value
  if (Array.isArray(value)) return value.map(item => {
    if (item && typeof item === 'object' && 'msg' in item) return String(item.msg)
    return 'Alan doğrulanamadı.'
  }).join(' · ')
  if (value && typeof value === 'object' && 'message' in value) return String(value.message)
  return 'İşlem tamamlanamadı. Bağlantı ve kasa durumunu kontrol edin.'
}

export default function ExchangeConnections() {
  const [status,setStatus] = useState<ConnectionStatus|null>(null)
  const [selected,setSelected] = useState<Mode>('TESTNET')
  const [forms,setForms] = useState<Record<Mode,FormState>>({TESTNET:emptyForm(),LIVE:emptyForm()})
  const [busy,setBusy] = useState('')
  const [notice,setNotice] = useState<{kind:'ok'|'warn'|'error';text:string}>({kind:'warn',text:'Şifreli borsa kasası kontrol ediliyor…'})

  const call = async <T,>(path:string,options:RequestInit={}):Promise<T> => {
    const headers = new Headers(options.headers)
    if (options.body) headers.set('Content-Type','application/json')
    const ownerToken = ownerAccessToken()
    if (ownerToken) headers.set('X-ProTreBot-Owner',ownerToken)
    const controller = new AbortController()
    const timeout = window.setTimeout(() => controller.abort(),15000)
    let response:Response
    try {
      response = await fetch(`${API_BASE}/exchange-connections${path}`,{...options,headers,signal:controller.signal})
    } catch (error) {
      if (error instanceof DOMException && error.name === 'AbortError') throw new Error('Borsa kasası yanıt vermedi. Backend ve PostgreSQL bağlantısını kontrol edin.')
      throw error
    } finally {
      window.clearTimeout(timeout)
    }
    const payload = await response.json().catch(() => null) as T|{detail?:unknown}|null
    if (!response.ok) {
      const detail = payload && typeof payload === 'object' && 'detail' in payload ? payload.detail : payload
      throw new Error(errorText(detail))
    }
    return payload as T
  }

  const refresh = async (quiet=false) => {
    try {
      const next = await call<ConnectionStatus>('/status')
      setStatus(next)
      if (!quiet) setNotice({kind:next.vault.ready ? 'ok' : 'warn',text:next.vault.ready ? 'Şifreli kasa ve iki Binance kanalı yenilendi.' : next.vault.reason || 'Kasa bekleniyor.'})
    } catch (error) {
      setNotice({kind:'error',text:error instanceof Error ? error.message : 'Kasa durumu alınamadı.'})
    }
  }

  useEffect(() => {
    void refresh(true)
    const timer = window.setInterval(() => void refresh(true),15000)
    return () => window.clearInterval(timer)
  },[])

  const form = forms[selected]
  const connection = status?.connections[selected]
  const patchForm = (patch:Partial<FormState>) => setForms(current => ({...current,[selected]:{...current[selected],...patch}}))

  const execute = async (key:string,action:()=>Promise<void>) => {
    setBusy(key)
    try {await action()} catch (error) {
      setNotice({kind:'error',text:error instanceof Error ? error.message : 'İşlem tamamlanamadı.'})
    } finally {setBusy('')}
  }

  const testConnection = () => execute(`test-${selected}`,async () => {
    if ((form.apiKey && !form.secretKey) || (!form.apiKey && form.secretKey)) throw new Error('API Key ve Secret Key birlikte girilmelidir.')
    const body = form.apiKey && form.secretKey
      ? {mode:selected,api_key:form.apiKey,secret_key:form.secretKey}
      : {mode:selected}
    const result = await call<{ok:boolean;message:string;account:AccountSummary}>('/test',{method:'POST',body:JSON.stringify(body)})
    setNotice({kind:'ok',text:`${result.message} Bakiye ${money(result.account.wallet_balance)} USDT.`})
  })

  const saveConnection = () => execute(`save-${selected}`,async () => {
    if (!form.apiKey || !form.secretKey) throw new Error('API Key ve Secret Key alanlarını doldurun.')
    if (!form.accepted) throw new Error('Güvenli saklama açıklamasını okuyup onay kutusunu işaretleyin.')
    const confirmation = selected === 'TESTNET' ? 'TESTNET KASAYA KAYDET' : 'CANLI KASAYA KAYDET'
    const next = await call<ConnectionStatus & {message:string}>('/save',{method:'POST',body:JSON.stringify({mode:selected,api_key:form.apiKey,secret_key:form.secretKey,confirmation})})
    setStatus(next)
    setForms(current => ({...current,[selected]:emptyForm()}))
    setNotice({kind:'ok',text:next.message})
  })

  const activateConnection = () => execute(`activate-${selected}`,async () => {
    if (selected === 'LIVE' && !window.confirm('Bu işlem yalnızca gerçek hesabın salt-okunur bağlantısını açar. Emir kilidi ayrıca kapalı kalacaktır. Devam edilsin mi?')) return
    const confirmation = selected === 'TESTNET' ? 'TESTNET BAĞLANTIYI AÇ' : 'CANLI SALT OKUNUR BAĞLANTIYI AÇ'
    const next = await call<ConnectionStatus & {message:string}>('/activate',{method:'POST',body:JSON.stringify({mode:selected,confirmation})})
    setStatus(next)
    const connectPath = selected === 'TESTNET' ? '/binance-demo/connect' : '/v25/connect/read-only'
    const connectHeaders = new Headers()
    const ownerToken = ownerAccessToken()
    if (ownerToken) connectHeaders.set('X-ProTreBot-Owner',ownerToken)
    const connectResponse = await fetch(`${API_BASE}${connectPath}`,{method:'POST',headers:connectHeaders})
    const connectPayload = await connectResponse.json().catch(() => null) as {detail?:unknown}|null
    if (!connectResponse.ok) throw new Error(`Kasa aktif ancak hesap merkezi bağlanamadı: ${errorText(connectPayload?.detail)}`)
    setNotice({kind:'ok',text:selected === 'TESTNET' ? 'Testnet kasası ve Demo hesap merkezi aktif. Emir kilidi yine ayrıca açılır.' : 'Gerçek hesap salt-okunur bağlandı. Gerçek emir kilidi ve otomasyon kapalı kaldı.'})
    await refresh(true)
  })

  const deactivateConnection = () => execute(`deactivate-${selected}`,async () => {
    const next = await call<ConnectionStatus & {message:string}>('/deactivate',{method:'POST',body:JSON.stringify({mode:selected,confirmation:'BAĞLANTIYI KAPAT'})})
    setStatus(next);setNotice({kind:'ok',text:next.message})
  })

  const deleteConnection = () => execute(`delete-${selected}`,async () => {
    if (!window.confirm(`${selected === 'TESTNET' ? 'Testnet' : 'Gerçek hesap'} şifreli anahtar kaydı kalıcı olarak silinsin mi?`)) return
    const next = await call<ConnectionStatus & {message:string}>('/credentials',{method:'DELETE',body:JSON.stringify({mode:selected,confirmation:'ANAHTARI KALICI SİL'})})
    setStatus(next);setForms(current => ({...current,[selected]:emptyForm()}));setNotice({kind:'ok',text:next.message})
  })

  const summary = connection?.account
  const badges = useMemo(() => [
    {ok:Boolean(status?.vault.ready),label:'ŞİFRELİ KASA'},
    {ok:Boolean(connection?.configured),label:'ANAHTAR KAYDI'},
    {ok:Boolean(connection?.last_test_ok),label:'İMZA TESTİ'},
    {ok:Boolean(connection?.active),label:'KANAL AKTİF'},
  ],[status,connection])

  return <div className="exchangeHub">
    <header className="exchangeHero">
      <div className="exchangeHeroIcon"><Link2/></div>
      <div><small>V28 · UYGULAMA İÇİ ŞİFRELİ KASA</small><h2>Borsa Bağlantıları Merkezi</h2><p>Testnet ve gerçek hesap anahtarlarını bu panelden test et, şifreli kaydet ve ayrı ayrı aktifleştir.</p></div>
      <aside className={status?.vault.ready ? 'ready' : 'wait'}><Database/><b>{status?.vault.ready ? 'KASA HAZIR' : 'KASA BEKLİYOR'}</b><span>{status?.vault.storage || 'PostgreSQL bağlantısı'}</span></aside>
    </header>

    <div className={`exchangeNotice ${notice.kind}`}><span>{notice.kind === 'ok' ? <CheckCircle2/> : <AlertTriangle/>}</span><b>{notice.text}</b><button onClick={() => refresh()} disabled={Boolean(busy)}><RefreshCw className={busy ? 'spin' : ''}/>YENİLE</button></div>

    <section className="exchangeModeSwitch">
      <button className={selected === 'TESTNET' ? 'active testnet' : ''} onClick={() => setSelected('TESTNET')}><TestTube2/><span><small>ÖNCE BURADA DENE</small><b>BINANCE FUTURES TESTNET</b><em>Sanal bakiye · gerçek para yok</em></span></button>
      <button className={selected === 'LIVE' ? 'active live' : ''} onClick={() => setSelected('LIVE')}><ShieldCheck/><span><small>SÜRELİ KİLİTLERLE KORUNUR</small><b>GERÇEK BINANCE FUTURES</b><em>Salt-okunur aktivasyon · emir ayrı kilitli</em></span></button>
    </section>

    <div className="exchangeGrid">
      <section className={`exchangeCard credentials ${selected.toLowerCase()}`}>
        <div className="exchangeCardHead"><div><KeyRound/><span><small>{selected} API KASASI</small><h3>Anahtar Ekle ve Doğrula</h3></span></div><b className={connection?.configured ? 'ok' : 'wait'}>{connection?.configured ? 'KAYITLI' : 'BOŞ'}</b></div>
        <p className="exchangeHint">Anahtarlar HTTPS ile doğrudan kendi sunucuna gider. Secret Key hiçbir API yanıtında geri gönderilmez ve arayüzde tekrar gösterilmez.</p>

        <label><span>API Key</span><div className="secretInput"><input type={form.showApi ? 'text' : 'password'} value={form.apiKey} onChange={event => patchForm({apiKey:event.target.value})} autoComplete="new-password" placeholder={connection?.configured ? 'Yeni anahtar girmeyeceksen boş bırak' : 'Binance API Key'}/><button onClick={() => patchForm({showApi:!form.showApi})} type="button">{form.showApi ? <EyeOff/> : <Eye/>}</button></div></label>
        <label><span>Secret Key</span><div className="secretInput"><input type={form.showSecret ? 'text' : 'password'} value={form.secretKey} onChange={event => patchForm({secretKey:event.target.value})} autoComplete="new-password" placeholder={connection?.configured ? 'Kayıtlı secret görüntülenmez' : 'Binance Secret Key'}/><button onClick={() => patchForm({showSecret:!form.showSecret})} type="button">{form.showSecret ? <EyeOff/> : <Eye/>}</button></div></label>

        <label className="exchangeCheck"><input type="checkbox" checked={form.accepted} onChange={event => patchForm({accepted:event.target.checked})}/><span>Secret’ın yalnızca şifreli PostgreSQL kasasında tutulacağını ve kaydettikten sonra tekrar görüntülenmeyeceğini anladım.</span></label>

        <div className="exchangeActions">
          <button className="secondary" onClick={testConnection} disabled={Boolean(busy) || !status?.vault.ready}>{busy === `test-${selected}` ? <LoaderCircle className="spin"/> : <Activity/>}BAĞLANTIYI TEST ET</button>
          <button className="primary" onClick={saveConnection} disabled={Boolean(busy) || !status?.vault.ready}>{busy === `save-${selected}` ? <LoaderCircle className="spin"/> : <LockKeyhole/>}ŞİFRELİ KAYDET</button>
        </div>

        <div className="exchangeFingerprint"><span>Anahtar izi</span><b>{connection?.fingerprint || '—'}</b><em>Secret gösterilmez</em></div>
      </section>

      <section className={`exchangeCard status ${selected.toLowerCase()}`}>
        <div className="exchangeCardHead"><div><WalletCards/><span><small>HESAP & YAYIN DURUMU</small><h3>{connection?.label || selected}</h3></span></div><b className={connection?.active ? 'ok' : 'wait'}>{connection?.active ? 'AKTİF' : 'KAPALI'}</b></div>
        <div className="exchangeBadges">{badges.map(item => <span key={item.label} className={item.ok ? 'ok' : 'wait'}>{item.ok ? <CheckCircle2/> : <LockKeyhole/>}{item.label}</span>)}</div>

        <div className="accountTiles">
          <article><small>CÜZDAN</small><b>{money(summary?.wallet_balance)} <em>USDT</em></b></article>
          <article><small>KULLANILABİLİR</small><b>{money(summary?.available_balance)} <em>USDT</em></b></article>
          <article><small>AÇIK PnL</small><b className={(summary?.unrealized_pnl || 0) < 0 ? 'loss' : 'gain'}>{money(summary?.unrealized_pnl)} <em>USDT</em></b></article>
          <article><small>POZİSYON</small><b>{summary?.active_positions ?? '—'}</b></article>
          <article><small>POZİSYON MODU</small><b>{summary ? (summary.hedge_mode ? 'HEDGE' : 'ONE-WAY') : '—'}</b></article>
          <article><small>SAAT FARKI</small><b>{summary?.clock_offset_ms ?? '—'} <em>ms</em></b></article>
        </div>

        <div className="connectionInfo"><span><b>Sunucu</b><code>{connection?.host || '—'}</code></span><span><b>Son test</b><em>{date(connection?.last_test_at)}</em></span>{connection?.last_error && <p><AlertTriangle/>{connection.last_error}</p>}</div>

        <div className="activationActions">
          {!connection?.active
            ? <button className="activate" onClick={activateConnection} disabled={Boolean(busy) || !connection?.configured || !connection?.last_test_ok}>{busy === `activate-${selected}` ? <LoaderCircle className="spin"/> : <Power/>}BAĞLANTIYI AKTİFLEŞTİR</button>
            : <button className="deactivate" onClick={deactivateConnection} disabled={Boolean(busy)}>{busy === `deactivate-${selected}` ? <LoaderCircle className="spin"/> : <PowerOff/>}BAĞLANTIYI KAPAT</button>}
          <button className="delete" onClick={deleteConnection} disabled={Boolean(busy) || !connection?.configured || connection?.active}>{busy === `delete-${selected}` ? <LoaderCircle className="spin"/> : <Trash2/>}ANAHTARI SİL</button>
        </div>
      </section>
    </div>

    <section className={`exchangeSafety ${selected.toLowerCase()}`}>
      <ShieldCheck/>
      <div><small>{selected === 'TESTNET' ? 'TESTNET İŞLEM ZİNCİRİ' : 'GERÇEK PARA GÜVENLİK ZİNCİRİ'}</small><h3>{selected === 'TESTNET' ? 'Aktivasyon bağlantıyı açar; 10 dakikalık Demo emir kilidi yine ayrıdır.' : 'Aktivasyon yalnızca hesabı salt-okunur bağlar; gerçek emir göndermez.'}</h3><p>{selected === 'TESTNET' ? 'Demo Komuta ekranındaki bağlantı, bakiye, pozisyon, Stop ve TP haritası bu anahtarı kullanır.' : 'Gerçek emir için Demo kanıtı, 24 saatlik risk izni, limit onayı ve yalnızca 5 dakikalık son kilit ayrıca geçmelidir.'}</p></div>
      <ul><li><CheckCircle2/>Para çekme desteği yok</li><li><CheckCircle2/>Secret yanıtta yok</li><li><CheckCircle2/>Test emri yok</li><li><CheckCircle2/>Aktivasyon emir açmaz</li></ul>
    </section>
  </div>
}

