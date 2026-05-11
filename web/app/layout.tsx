import './globals.css';

export const metadata = {
  title: '서울 실시간 혼잡도',
  description: '서울 핫스팟 120곳의 실시간 혼잡도 — Phase 1A 데모',
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="ko">
      <body>
        <div className="min-h-full">{children}</div>
        <footer className="px-4 py-2 text-xs text-zinc-500 border-t border-zinc-800">
          서울 도시데이터 · 지하철 도착정보 OpenAPI 기반 ·{' '}
          <a className="underline" href="/privacy">개인정보 처리방침</a>
        </footer>
      </body>
    </html>
  );
}
