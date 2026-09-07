import { createPortal } from 'react-dom'
import { type FormEvent, type ReactNode, useEffect, useRef, useState } from 'react'
import { Activity, ArrowRight, Eye, EyeOff, KeyRound, LogOut, MailCheck, ShieldCheck, UserRound } from 'lucide-react'
import { API_BASE, clearUserSessionToken, saveUserSessionToken, userSessionToken } from './api'
import AdminPanel from './AdminPanel'
import './auth.css'

type User = { id:string; email:string; display_name:string; role:string; active:boolean; email_verified?:boolean }
type Session = { user:User }
type Mode = 'login'|'register'|'forgot'|'reset'|'verify'
type ProfileData = {user:User;profile?:{full_name?:string;preferences?:Record<string,unknown>};subscription?:{plan?:string;status?:string;currentPeriodStart?:string;currentPeriodEnd?:string}}

function detail(payload:unknown):string {
  if (payload && typeof payload === 'object' && 'detail' in payload) {
    const value = (payload as {detail:unknown}).detail
    if (typeof value === 'string') return value
  }
  return 'İşlem tamamlanamadı. Bilgilerinizi kontrol edip tekrar deneyin.'
}

async function request<T>(path:string, options:RequestInit = {}):Promise<T> {
  const headers = new Headers(options.headers)
  if (options.body) headers.set('Content-Type','application/json')
  const response = await fetch(`${API_BASE}/v22${path}`, {...options, headers})
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    if (response.status === 409) throw new Error('Bu e-posta adresiyle zaten bir hesap bulunuyor.')
    if (response.status >= 500) throw new Error('Sunucuda geçici bir sorun oluştu. Lütfen biraz sonra tekrar deneyin.')
    throw new Error(detail(payload))
  }
  return payload as T
}

function strength(password:string):{label:string;score:number} {
  const score = [password.length >= 10, /[A-Z]/.test(password), /\d/.test(password), /[^A-Za-z0-9]/.test(password)].filter(Boolean).length
  return {score, label:score < 2 ? 'Zayıf' : score < 4 ? 'Orta' : 'Güçlü'}
}

function passwordRules(password:string) {
  return [
    ['En az 10 karakter', password.length >= 10],
    ['Büyük harf', /[A-Z]/.test(password)],
    ['Küçük harf', /[a-z]/.test(password)],
    ['Rakam', /\d/.test(password)],
    ['Sembol', /[^A-Za-z0-9]/.test(password)],
  ] as const
}

function ProfileSettings({token,user,onLogout}:{token:string;user:User;onLogout:()=>void}) {
  const [data,setData] = useState<ProfileData|null>(null)
  const [name,setName] = useState(user.display_name)
  const [passwords,setPasswords] = useState({current_password:'',new_password:''})
  const [notice,setNotice] = useState('')
  const [busy,setBusy] = useState(false)
  const call = async <T,>(path:string,options:RequestInit = {}):Promise<T> => { const headers = new Headers(options.headers); headers.set('Authorization',`Bearer ${token}`); if (options.body) headers.set('Content-Type','application/json'); const response = await fetch(`${API_BASE}/v22${path}`,{...options,headers}); const payload = await response.json().catch(() => null); if (!response.ok) throw new Error(detail(payload)); return payload as T }
  useEffect(() => { void call<ProfileData>('/profile').then(value => {setData(value);setName(value.profile?.full_name || value.user.display_name)}).catch(error => setNotice(error instanceof Error ? error.message : 'Profil yüklenemedi.')) },[])
  const saveProfile = async () => { setBusy(true); try { await call('/profile',{method:'PATCH',body:JSON.stringify({display_name:name,preferences:data?.profile?.preferences || {}})}); setNotice('Profil güncellendi.') } catch (error) {setNotice(error instanceof Error ? error.message : 'Profil güncellenemedi.')} finally {setBusy(false)} }
  const changePassword = async () => { setBusy(true); try { await call('/auth/change-password',{method:'POST',body:JSON.stringify(passwords)}); setPasswords({current_password:'',new_password:''}); setNotice('Parola güncellendi. Güvenlik için tekrar giriş yapın.'); setTimeout(onLogout,700) } catch (error) {setNotice(error instanceof Error ? error.message : 'Parola güncellenemedi.')} finally {setBusy(false)} }
  const deleteAccount = async () => { if (!window.confirm('Hesabınız kapatılacak ve tüm oturumlarınız sonlandırılacak. Devam edilsin mi?')) return; setBusy(true); try { await call('/profile',{method:'DELETE'}); onLogout() } catch (error) {setNotice(error instanceof Error ? error.message : 'Hesap kapatılamadı.')} finally {setBusy(false)} }
  return <main className="profilePage"><header className="profileHeader"><div><span>ACCOUNT / SETTINGS</span><h1>Profile &amp; Settings</h1><p>Kimlik, güvenlik ve üyelik durumunuzu yönetin.</p></div><button onClick={onLogout}><LogOut/> Çıkış</button></header>{notice && <p className="profileNotice">{notice}</p>}<div className="profileGrid"><section className="profileCard"><span>PROFILE</span><h2>Personal details</h2><label>Full name<input value={name} onChange={event => setName(event.target.value)}/></label><label>Email<input value={user.email} readOnly/></label><div className="profileVerified"><MailCheck/> {user.email_verified === false ? 'Email verification required' : 'Email verified'}</div><button disabled={busy} onClick={() => void saveProfile()}>SAVE PROFILE</button></section><section className="profileCard"><span>SECURITY</span><h2>Change password</h2><label>Current password<input type="password" value={passwords.current_password} onChange={event => setPasswords({...passwords,current_password:event.target.value})}/></label><label>New password<input type="password" value={passwords.new_password} onChange={event => setPasswords({...passwords,new_password:event.target.value})}/></label><button disabled={busy} onClick={() => void changePassword()}>UPDATE PASSWORD</button><p className="profileMuted">Session: signed bearer token · {user.role === 'OWNER' ? 'Admin' : 'Member'}</p></section><section className="profileCard"><span>SUBSCRIPTION</span><h2>Current membership</h2><div className="profileFacts"><b>Plan <strong>{data?.subscription?.plan || 'FREE'}</strong></b><b>Status <strong>{data?.subscription?.status || 'inactive'}</strong></b><b>Started <strong>{data?.subscription?.currentPeriodStart ? new Date(data.subscription.currentPeriodStart).toLocaleDateString('tr-TR') : '—'}</strong></b><b>Expires <strong>{data?.subscription?.currentPeriodEnd ? new Date(data.subscription.currentPeriodEnd).toLocaleDateString('tr-TR') : '—'}</strong></b></div></section><section className="profileCard profileDanger"><span>ACCOUNT</span><h2>Close account</h2><p>Hesap kapatıldığında oturumlar geçersiz kılınır ve erişim durdurulur.</p><button disabled={busy || user.role === 'OWNER'} onClick={() => void deleteAccount()}>DELETE ACCOUNT</button></section></div></main>
}

export default function AuthGate({children}:{children:ReactNode}) {
  const [token,setToken] = useState(userSessionToken())
  const [session,setSession] = useState<Session|null>(null)
  const [mode,setMode] = useState<Mode>(() => window.location.pathname === '/register' ? 'register' : window.location.pathname === '/forgot-password' ? 'forgot' : window.location.pathname === '/reset-password' ? 'reset' : window.location.pathname === '/verify-email' ? 'verify' : 'login')
  const [busy,setBusy] = useState(true)
  const [message,setMessage] = useState('Oturum doğrulanıyor…')
  const [showPassword,setShowPassword] = useState(false)
  const [remember,setRemember] = useState(true)
  const [login,setLogin] = useState({email:'',password:''})
  const [register,setRegister] = useState({display_name:'',email:'',password:'',confirm_password:'',terms_accepted:false})
  const [email,setEmail] = useState('')
  const queryToken = new URLSearchParams(window.location.search).get('token') || ''
  const [resetToken,setResetToken] = useState(window.location.pathname === '/reset-password' ? queryToken : '')
  const [verificationLinkToken,setVerificationLinkToken] = useState(window.location.pathname === '/verify-email' ? queryToken : '')
  const [verificationStatusToken,setVerificationStatusToken] = useState('')
  const [verificationInput,setVerificationInput] = useState('')
  const [resetPassword,setResetPassword] = useState({password:'',confirm_password:''})
  const [fieldErrors,setFieldErrors] = useState<Record<string,string>>({})
  const [termsError,setTermsError] = useState('')
  const [verificationNotice,setVerificationNotice] = useState(false)
  const autoVerificationStarted = useRef(false)
  const [autoVerifying,setAutoVerifying] = useState(false)
  const [memberMenuOpen,setMemberMenuOpen] = useState(false)
  const memberTriggerRef = useRef<HTMLButtonElement>(null)
  const memberMenuRef = useRef<HTMLDivElement>(null)
  const [memberMenuPosition,setMemberMenuPosition] = useState({top:0,left:12})

  useEffect(() => {
    if (!memberMenuOpen) return
    const updateMenuPosition = () => {
      const trigger = memberTriggerRef.current
      if (!trigger) return
      const rect = trigger.getBoundingClientRect()
      const menuWidth = window.innerWidth <= 600 ? 300 : 276
      setMemberMenuPosition({top:rect.bottom + 9,left:Math.min(Math.max(12,rect.right - menuWidth),Math.max(12,window.innerWidth - menuWidth - 12))})
    }
    const closeMenu = (event:MouseEvent) => {
      const target = event.target as Node
      if (!memberTriggerRef.current?.contains(target) && !memberMenuRef.current?.contains(target)) setMemberMenuOpen(false)
    }
    const closeOnEscape = (event:KeyboardEvent) => { if (event.key === 'Escape') setMemberMenuOpen(false) }
    updateMenuPosition()
    document.addEventListener('mousedown',closeMenu)
    document.addEventListener('keydown',closeOnEscape)
    window.addEventListener('resize',updateMenuPosition)
    window.addEventListener('scroll',updateMenuPosition,true)
    return () => { document.removeEventListener('mousedown',closeMenu); document.removeEventListener('keydown',closeOnEscape); window.removeEventListener('resize',updateMenuPosition); window.removeEventListener('scroll',updateMenuPosition,true) }
  },[memberMenuOpen])

  const loadSession = async (value:string) => {
    if (!value) { setBusy(false); return }
    try {
      const current = await request<Session>('/session',{headers:{Authorization:`Bearer ${value}`}})
      setSession(current); setMessage('')
    } catch {
      clearUserSessionToken(); setToken(''); setSession(null); setMessage('Oturum açmak için devam edin.')
    } finally { setBusy(false) }
  }

  useEffect(() => { void loadSession(token) }, [])

  useEffect(() => {
    if (mode !== 'verify' || autoVerificationStarted.current) return
    if (!verificationLinkToken && !verificationStatusToken) return
    autoVerificationStarted.current = true
    setAutoVerifying(true)
    let active = true
    const verifyEmailLink = async () => {
      try {
        await request('/auth/verify-email',{method:'POST',body:JSON.stringify({token:verificationLinkToken})})
        if (active) { setVerificationLinkToken(''); setMessage('E-posta doğrulandı. Şimdi giriş yapabilirsiniz.'); setMode('login') }
      } catch (error) {
        if (active) setMessage(error instanceof Error ? error.message : 'İşlem başarısız.')
      }
      if (active) setAutoVerifying(false)
    }
    const checkStatus = async () => {
      try {
        const result = await request<{verified:boolean}>(`/auth/verification-status?token=${encodeURIComponent(verificationStatusToken)}`)
        if (active && result.verified) {
          setVerificationStatusToken(''); setMessage('E-posta doğrulandı. Şimdi giriş yapabilirsiniz.'); setMode('login')
        }
      } catch (error) {
        if (active) setMessage(error instanceof Error ? error.message : 'İşlem başarısız.')
      } finally {
        if (active) setAutoVerifying(false)
      }
    }
    if (verificationLinkToken) void verifyEmailLink()
    else {
      void checkStatus()
      const interval = window.setInterval(() => void checkStatus(),1000)
      return () => { active = false; window.clearInterval(interval) }
    }
    return () => { active = false }
  },[mode,verificationLinkToken,verificationStatusToken])

  const validateForm = ():boolean => {
    const errors:Record<string,string> = {}
    const emailValue = mode === 'login' ? login.email : mode === 'register' || mode === 'forgot' ? (mode === 'register' ? register.email : email) : ''
    if (mode === 'login' || mode === 'register' || mode === 'forgot') {
      if (!emailValue.trim()) errors.email = 'E-posta adresinizi girin.'
      else if (!/^\S+@\S+\.\S+$/.test(emailValue)) errors.email = 'Lütfen geçerli bir e-posta adresi girin.'
    }
    if (mode === 'login' && !login.password) errors.password = 'Şifrenizi girin.'
    if (mode === 'register') {
      if (!register.password) errors.password = 'Şifrenizi girin.'
      else if (!passwordRules(register.password).every(([,passed]) => passed)) errors.password = 'Şifreniz tüm güvenlik şartlarını karşılamıyor.'
      if (!register.confirm_password) errors.confirm_password = 'Şifrenizi tekrar girin.'
      else if (register.password !== register.confirm_password) errors.confirm_password = 'Şifreler eşleşmiyor.'
      if (!register.terms_accepted) setTermsError('Devam etmek için kullanım koşullarını ve gizlilik politikasını kabul etmelisiniz.')
      else setTermsError('')
    }
    if (mode === 'verify' && !verificationInput && !verificationStatusToken && !verificationLinkToken) errors.verification = 'Doğrulama kodunu girin.'
    setFieldErrors(errors)
    return !Object.keys(errors).length && (mode !== 'register' || register.terms_accepted)
  }

  const submit = async (event:FormEvent) => {
    event.preventDefault()
    if (!validateForm()) return
    setBusy(true); setMessage('')
    try {
      if (mode === 'login') {
        const result = await request<{token:string;user:User}>('/auth/login',{method:'POST',body:JSON.stringify({...login,remember})})
        if (result.user.role !== 'OWNER' && result.user.email_verified === false) {
          setEmail(result.user.email); setResetToken(''); setMode('verify'); setMessage('Önce e-posta adresinizi doğrulayın. Doğrulama kodunu e-postanızdan alın.')
        } else {
          saveUserSessionToken(result.token,remember); setMemberMenuOpen(false); setToken(result.token); setSession({user:result.user})
        }
      } else if (mode === 'register') {
        const result = await request<{verification_status_token?:string;message:string}>('/auth/register',{method:'POST',body:JSON.stringify(register)})
        setEmail(register.email); setVerificationStatusToken(result.verification_status_token || ''); setVerificationInput(''); setVerificationNotice(true); setMode('verify'); setMessage(result.message)
      } else if (mode === 'forgot') {
        const result = await request<{development_reset_token?:string;message:string}>('/auth/forgot-password',{method:'POST',body:JSON.stringify({email})})
        if (result.development_reset_token) setResetToken(result.development_reset_token)
        setMessage(result.message); if (result.development_reset_token) setMode('reset')
      } else if (mode === 'reset') {
        await request('/auth/reset-password',{method:'POST',body:JSON.stringify({token:resetToken,...resetPassword})})
        setMessage('Parolanız güncellendi. Giriş yapabilirsiniz.'); setMode('login')
      } else {
        await request('/auth/verify-email',{method:'POST',body:JSON.stringify({token:verificationInput})})
        setVerificationNotice(false); setMessage('E-posta doğrulandı. Şimdi giriş yapabilirsiniz.'); setMode('login')
      }
    } catch (error) { setMessage(error instanceof Error ? error.message : 'İşlem başarısız.') }
    finally { setBusy(false) }
  }

  const logout = async () => {
    try { if (token) await request('/auth/logout',{method:'POST',headers:{Authorization:`Bearer ${token}`}}) } catch { /* local logout still clears the session */ }
    clearUserSessionToken(); setMemberMenuOpen(false); setToken(''); setSession(null); setMode('login'); setMessage('Oturum kapatıldı.')
  }

  if (busy && !session) return <main className="authLoading"><div className="authLoader"><ShieldCheck/><b>GÜVENLİ OTURUM</b><span>Hesap durumu kontrol ediliyor…</span></div></main>
  if (autoVerifying) return <main className="authLoading"><div className="authLoader"><MailCheck/><b>E-POSTA DOĞRULANIYOR</b><span>E-posta doğrulanıyor...</span></div></main>
  if (!session) {
    const registrationStrength = strength(register.password)
    return <main className="authPage">
      <section className="authIntro"><div className="authBrand"><span className="authBrandMark">X</span><div><b>PROTREBOT</b><strong>ELITE X</strong></div></div><span className="authEyebrow">SECURE TRADING WORKSPACE</span><h1>Trading operasyonlarınızı daha kontrollü yönetin.</h1><p>Demo, analiz ve risk araçlarını tek bir güvenli hesapla yönetin. Sade, kontrollü ve gerçek para hareketinden ayrıştırılmış bir trading çalışma alanı.</p><div className="authProof"><span><ShieldCheck/> Güvenli hesap sistemi</span><span><KeyRound/> Hızlı erişim</span><span><Activity/> Kontrollü trading altyapısı</span></div><div className="authAtmosphere"><i/><i/><i/><i/><i/><i/></div></section>
      <section className={`authCard ${mode === 'verify' ? 'authCardVerify' : ''}`}>
        <div className="authCardHead"><div className="authMark">{mode === 'verify' ? <MailCheck/> : <UserRound/>}</div><div><span>MEMBER ACCESS</span><h2>{mode === 'login' ? 'Hesabınıza giriş yapın' : mode === 'register' ? 'Hesabınızı oluşturun' : mode === 'forgot' ? 'Parolanızı yenileyin' : mode === 'reset' ? 'Yeni parola belirleyin' : 'E-postanızı kontrol edin'}</h2></div></div>
        <p className="authCardLead">{mode === 'login' ? 'ProTreBot Elite X çalışma alanınıza güvenli şekilde erişin.' : mode === 'register' ? "ProTreBot Elite X'e katılın ve çalışma alanınızı oluşturun." : mode === 'forgot' ? 'Hesabınıza yeniden erişmek için güvenli bir bağlantı gönderelim.' : mode === 'reset' ? 'Yeni ve güçlü bir parola belirleyin.' : 'Gelen kutunuzdaki bağlantıyla hesabınızı güvenle etkinleştirin.'}</p>
        <form onSubmit={submit}>
          {mode === 'login' && <><label className={fieldErrors.email ? 'authFieldError' : ''}>E-posta<input type="email" required autoComplete="username" value={login.email} onChange={event => {setLogin({...login,email:event.target.value});setFieldErrors(current => ({...current,email:''}))}} placeholder="siz@ornek.com"/>{fieldErrors.email && <em>{fieldErrors.email}</em>}</label><label className={fieldErrors.password ? 'authFieldError' : ''}>Parola<div className="authPassword"><input required type={showPassword ? 'text' : 'password'} autoComplete="current-password" value={login.password} onChange={event => {setLogin({...login,password:event.target.value});setFieldErrors(current => ({...current,password:''}))}}/><button type="button" onClick={() => setShowPassword(value => !value)} aria-label="Parolayı göster veya gizle">{showPassword ? <EyeOff/> : <Eye/>}</button></div>{fieldErrors.password && <em>{fieldErrors.password}</em>}</label><label className="authCheck"><input type="checkbox" checked={remember} onChange={event => setRemember(event.target.checked)}/><span>Bu cihazda oturumu hatırla</span></label></>}
          {mode === 'register' && <><label>Ad soyad<input required autoComplete="name" value={register.display_name} onChange={event => setRegister({...register,display_name:event.target.value})} placeholder="Ada Yılmaz"/></label><label className={fieldErrors.email ? 'authFieldError' : ''}>E-posta<input type="email" autoComplete="email" value={register.email} onChange={event => {setRegister({...register,email:event.target.value});setFieldErrors(current => ({...current,email:''}))}} placeholder="siz@ornek.com"/>{fieldErrors.email && <em>{fieldErrors.email}</em>}</label><label className={fieldErrors.password ? 'authFieldError' : ''}>Parola<div className="authPassword"><input type={showPassword ? 'text' : 'password'} autoComplete="new-password" value={register.password} onChange={event => {setRegister({...register,password:event.target.value});setFieldErrors(current => ({...current,password:''}))}}/><button type="button" onClick={() => setShowPassword(value => !value)} aria-label="Parolayı göster veya gizle">{showPassword ? <EyeOff/> : <Eye/>}</button></div><div className={`passwordMeter strength${registrationStrength.score}`}><i style={{width:`${registrationStrength.score * 20}%`}}/><span>{registrationStrength.label} · tüm şartlar sağlanmalı</span></div><div className="passwordRules">{passwordRules(register.password).map(([label,passed]) => <span className={passed ? 'passed' : 'missing'} key={label}>{passed ? '✓' : '•'} {label}</span>)}</div>{fieldErrors.password && <em>{fieldErrors.password}</em>}</label><label className={fieldErrors.confirm_password ? 'authFieldError' : ''}>Parola tekrar<input type="password" autoComplete="new-password" value={register.confirm_password} onChange={event => {setRegister({...register,confirm_password:event.target.value});setFieldErrors(current => ({...current,confirm_password:''}))}}/>{fieldErrors.confirm_password && <em>{fieldErrors.confirm_password}</em>}</label><label className={`authCheck ${termsError ? 'authCheckError' : ''}`}><input type="checkbox" checked={register.terms_accepted} onChange={event => {setRegister({...register,terms_accepted:event.target.checked});setTermsError('')}}/><span>Kullanım koşullarını ve gizlilik politikasını kabul ediyorum.</span></label>{termsError && <p className="authInlineError">{termsError}</p>}</>}
          {mode === 'forgot' && <label>E-posta<input required type="email" autoComplete="email" value={email} onChange={event => setEmail(event.target.value)} placeholder="siz@ornek.com"/><small>Kayıtlıysa yenileme bağlantısı hazırlanır.</small></label>}
          {mode === 'verify' && <label className={fieldErrors.verification ? 'authFieldError' : ''}>Doğrulama kodu<input required value={verificationInput} onChange={event => {setVerificationInput(event.target.value);setFieldErrors(current => ({...current,verification:''}))}} placeholder="E-posta doğrulama kodu"/><small>{message || 'Kayıt sonrası e-postanızdaki kodu girin.'}</small>{fieldErrors.verification && <em>{fieldErrors.verification}</em>}</label>}
          {mode === 'reset' && <><label>Doğrulama kodu<input required value={resetToken} onChange={event => setResetToken(event.target.value)}/></label><label>Yeni parola<input required type="password" autoComplete="new-password" value={resetPassword.password} onChange={event => setResetPassword({...resetPassword,password:event.target.value})}/></label><label>Yeni parola tekrar<input required type="password" autoComplete="new-password" value={resetPassword.confirm_password} onChange={event => setResetPassword({...resetPassword,confirm_password:event.target.value})}/></label></>}
          <button className="authSubmit" disabled={busy}>{busy ? 'İŞLENİYOR…' : mode === 'login' ? 'GÜVENLİ GİRİŞ' : mode === 'register' ? 'HESAP OLUŞTUR' : mode === 'forgot' ? 'YENİLEME BAĞLANTISI GÖNDER' : mode === 'reset' ? 'PAROLAYI GÜNCELLE' : 'E-POSTAYI DOĞRULA'}<ArrowRight/></button>
        </form>
        {mode === 'verify' && verificationNotice && <div className="authVerificationPanel"><b>Doğrulama bağlantısını e-posta adresinize gönderdik.</b><span>E-postayı göremiyor musunuz? Spam / Gereksiz / Tanıtımlar klasörünü de kontrol edin.</span><small>Doğrulama bekleniyor... Bu sayfa başka cihazdan yapılan doğrulamayı otomatik algılar.</small></div>}
        {message && message !== 'Oturum doğrulanıyor…' && <p className="authMessage">{message}</p>}
        <div className="authLinks">{mode === 'login' && <><button onClick={() => setMode('forgot')}>Parolamı unuttum</button><button onClick={() => setMode('register')}>Yeni hesap oluştur</button></>}{mode !== 'login' && <button onClick={() => setMode('login')}>Giriş ekranına dön</button>}</div>
      </section>
    </main>
  }

  const path = window.location.pathname
  if (['/login','/register','/forgot-password','/reset-password','/verify-email'].includes(path)) { history.replaceState(null,'','/dashboard') }
  if (path.startsWith('/admin') && session.user.role !== 'OWNER') return <main className="authLoading"><div className="authLoader"><ShieldCheck/><b>403 · ERİŞİM YOK</b><span>Bu alan yalnızca yönetici hesaplarına açıktır.</span><button onClick={() => {history.replaceState(null,'','/dashboard'); location.reload()}}>Dashboard'a dön</button></div></main>
  if (path.startsWith('/admin')) return <><div className="authSessionBar"><span><ShieldCheck/> {session.user.display_name} <b>ADMIN</b></span><button onClick={() => void logout()}><LogOut/> Çıkış</button></div><AdminPanel token={token} onBack={() => {history.replaceState(null,'','/dashboard');location.reload()}}/></>
  if (path.startsWith('/settings')) return <><div className="authSessionBar"><span><ShieldCheck/> {session.user.display_name} <b>{session.user.role === 'OWNER' ? 'ADMIN' : 'MEMBER'}</b></span><button onClick={() => void logout()}><LogOut/> Çıkış</button></div><ProfileSettings token={token} user={session.user} onLogout={() => void logout()}/></>
  const memberMenu = memberMenuOpen ? createPortal(<div ref={memberMenuRef} className="authMemberMenu authMemberPortalMenu" role="menu" style={{top:memberMenuPosition.top,left:memberMenuPosition.left}}><div className="authMemberMenuHead"><small>SECURE ACCOUNT</small><strong>{session.user.email}</strong></div>{session.user.role === 'OWNER' && <button type="button" role="menuitem" onClick={() => {setMemberMenuOpen(false);location.assign('/admin')}}><ShieldCheck/><span><b>Admin Dashboard</b><small>Control center</small></span></button>}<button type="button" role="menuitem" onClick={() => {setMemberMenuOpen(false);location.assign('/settings')}}><UserRound/><span><b>Profile &amp; Settings</b><small>Identity and security</small></span></button><button className="authMemberLogout" type="button" role="menuitem" onClick={() => void logout()}><LogOut/><span><b>Çıkış</b><small>End secure session</small></span></button></div>,document.body) : null
  return <><div className="authSessionBar"><button ref={memberTriggerRef} className="authMemberTrigger" type="button" aria-label="Profil menüsünü aç" aria-expanded={memberMenuOpen} aria-haspopup="menu" onClick={() => setMemberMenuOpen(value => !value)}><ShieldCheck/></button></div>{memberMenu}{children}</>
}
