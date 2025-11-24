import { NextResponse } from 'next/server'
import { checkHealth } from '@/services/api'

export async function GET() {
  try {
    const health = await checkHealth()
    return NextResponse.json(health)
  } catch (error) {
    return NextResponse.json(
      { status: 'unhealthy', error: 'Backend not reachable' },
      { status: 503 }
    )
  }
}
