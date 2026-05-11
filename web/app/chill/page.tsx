import ChillList from '@/components/ChillList';

export const metadata = {
  title: '한가하고 영업 중 — 서울',
  description: '서울 자치구 혼잡도 낮은 곳 + 현재 영업 중 가게 — Phase 1A 데모',
};

export default function ChillPage() {
  return (
    <main>
      <header className="px-4 py-3 border-b border-zinc-800 flex items-center justify-between">
        <h1 className="text-lg font-semibold">지금 한가하고 영업 중</h1>
        <a
          href="/"
          className="text-sm text-zinc-400 hover:text-zinc-100 underline"
        >
          &larr; 메인 지도
        </a>
      </header>
      <ChillList />
    </main>
  );
}
