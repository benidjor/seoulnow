'use client';

import dynamic from 'next/dynamic';

const HotspotMap = dynamic(() => import('@/components/HotspotMap'), { ssr: false });

export default function Page() {
  return (
    <main>
      <header className="px-4 py-3 border-b border-zinc-800 flex items-center justify-between">
        <h1 className="text-lg font-semibold">서울 핫스팟 실시간 혼잡도</h1>
        <a href="/chill/" className="text-sm text-zinc-400 hover:text-zinc-100 underline">
          지금 한가하고 영업 중인 곳 &rarr;
        </a>
      </header>
      <HotspotMap />
    </main>
  );
}
