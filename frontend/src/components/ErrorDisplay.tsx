'use client'

import { AlertCircle, X } from 'lucide-react'

interface ErrorDisplayProps {
  error: string
  onDismiss: () => void
}

export default function ErrorDisplay({ error, onDismiss }: ErrorDisplayProps) {
  return (
    <div className="mb-6 bg-red-50 border-l-4 border-red-500 p-4 rounded-r-lg">
      <div className="flex items-start justify-between">
        <div className="flex items-start space-x-3">
          <AlertCircle className="w-5 h-5 text-red-500 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-sm font-medium text-red-800">Error</h3>
            <p className="text-sm text-red-700 mt-1">{error}</p>
          </div>
        </div>
        <button
          onClick={onDismiss}
          className="text-red-500 hover:text-red-700 transition-colors"
        >
          <X className="w-5 h-5" />
        </button>
      </div>
    </div>
  )
}
