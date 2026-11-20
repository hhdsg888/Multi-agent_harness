"""OS memory/process limits for candidate workers; not an authority sandbox."""
import json,os,subprocess,tempfile,time

def windows_job(process):
    import ctypes as c
    from ctypes import wintypes as w
    class IO(c.Structure):
        _fields_=[(n,c.c_ulonglong) for n in ('ReadOperationCount','WriteOperationCount','OtherOperationCount','ReadTransferCount','WriteTransferCount','OtherTransferCount')]
    class BASIC(c.Structure):
        _fields_=[('PerProcessUserTimeLimit',c.c_longlong),('PerJobUserTimeLimit',c.c_longlong),
            ('LimitFlags',w.DWORD),('MinimumWorkingSetSize',c.c_size_t),('MaximumWorkingSetSize',c.c_size_t),
            ('ActiveProcessLimit',w.DWORD),('Affinity',c.c_size_t),('PriorityClass',w.DWORD),('SchedulingClass',w.DWORD)]
    class EXTENDED(c.Structure):
        _fields_=[('BasicLimitInformation',BASIC),('IoInfo',IO),('ProcessMemoryLimit',c.c_size_t),
            ('JobMemoryLimit',c.c_size_t),('PeakProcessMemoryUsed',c.c_size_t),('PeakJobMemoryUsed',c.c_size_t)]
    k=c.WinDLL('kernel32',use_last_error=True)
    k.CreateJobObjectW.argtypes=[c.c_void_p,w.LPCWSTR];k.CreateJobObjectW.restype=w.HANDLE
    k.SetInformationJobObject.argtypes=[w.HANDLE,c.c_int,c.c_void_p,w.DWORD];k.SetInformationJobObject.restype=w.BOOL
    k.AssignProcessToJobObject.argtypes=[w.HANDLE,w.HANDLE];k.AssignProcessToJobObject.restype=w.BOOL
    k.CloseHandle.argtypes=[w.HANDLE]
    job=k.CreateJobObjectW(None,None)
    if not job:raise OSError(c.get_last_error(),'CreateJobObject')
    limits=EXTENDED();limits.BasicLimitInformation.LimitFlags=0x2000|0x100|0x8
    limits.BasicLimitInformation.ActiveProcessLimit=1;limits.ProcessMemoryLimit=128*1024*1024
    if not k.SetInformationJobObject(job,9,c.byref(limits),c.sizeof(limits)) or not k.AssignProcessToJobObject(job,w.HANDLE(int(process._handle))):
        error=c.get_last_error();k.CloseHandle(job);raise OSError(error,'Candidate resource boundary unavailable')
    return lambda:k.CloseHandle(job)

def run_candidate(command,payload,cwd,timeout=5):
    raw=json.dumps(payload,ensure_ascii=True).encode('utf-8')
    if len(raw)>512*1024:raise ValueError('候选输入超过512KiB')
    with tempfile.TemporaryFile() as out,tempfile.TemporaryFile() as err:
        kwargs={'stdin':subprocess.PIPE,'stdout':out,'stderr':err,'cwd':cwd}
        if os.name=='nt':kwargs['creationflags']=getattr(subprocess,'CREATE_NO_WINDOW',0)
        else:
            def limits():
                import resource
                resource.setrlimit(resource.RLIMIT_AS,(128*1024*1024,128*1024*1024))
                resource.setrlimit(resource.RLIMIT_CPU,(5,5))
            kwargs['preexec_fn']=limits
        p=subprocess.Popen(command,**kwargs);release=lambda:None
        try:
            # Worker blocks on stdin: attach OS limits before sending any source.
            if os.name=='nt':release=windows_job(p)
            p.stdin.write(raw);p.stdin.close();deadline=time.monotonic()+timeout
            while p.poll() is None:
                if time.monotonic()>deadline:raise subprocess.TimeoutExpired(command,timeout)
                if os.fstat(out.fileno()).st_size+os.fstat(err.fileno()).st_size>1024*1024:raise ValueError('候选输出超过1MiB')
                time.sleep(.02)
            if os.fstat(out.fileno()).st_size+os.fstat(err.fileno()).st_size>1024*1024:raise ValueError('候选输出超过1MiB')
            out.seek(0);err.seek(0)
            return subprocess.CompletedProcess(command,p.returncode,out.read().decode('utf-8'),err.read().decode('utf-8',errors='replace'))
        finally:
            if p.poll() is None:p.kill()
            p.wait();release()
            if p.stdin and not p.stdin.closed:p.stdin.close()
