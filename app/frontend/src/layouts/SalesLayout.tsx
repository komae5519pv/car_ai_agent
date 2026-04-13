import { Outlet, NavLink } from 'react-router-dom'
import { FiUsers, FiSettings, FiUser, FiBarChart2 } from 'react-icons/fi'
import { LuCar } from 'react-icons/lu'
import { HiOutlineSparkles } from 'react-icons/hi2'
import clsx from 'clsx'
import { useCurrentUser } from '../context/CurrentUserContext'

export function SalesLayout() {
  const currentUser = useCurrentUser()

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Sidebar */}
      <aside className="w-64 bg-white border-r border-gray-200 flex flex-col">
        {/* Logo */}
        <div className="h-16 flex items-center px-6 border-b border-gray-200">
          <LuCar className="w-8 h-8 text-blue-600" />
          <span className="ml-3 text-xl font-bold text-gray-900">Car AI Demo</span>
        </div>

        {/* Navigation */}
        <nav className="flex-1 px-4 py-6 space-y-2">
          <NavLink
            to="/sales"
            end
            className={({ isActive }) =>
              clsx(
                'flex items-center px-4 py-3 text-sm font-medium rounded-lg transition-colors',
                isActive
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
              )
            }
          >
            <FiUsers className="w-5 h-5 mr-3" />
            顧客一覧
          </NavLink>
          <NavLink
            to="/sales/mypage"
            className={({ isActive }) =>
              clsx(
                'flex items-center px-4 py-3 text-sm font-medium rounded-lg transition-colors',
                isActive
                  ? 'bg-blue-50 text-blue-700'
                  : 'text-gray-600 hover:bg-gray-50 hover:text-gray-900'
              )
            }
          >
            <FiBarChart2 className="w-5 h-5 mr-3" />
            マイページ
          </NavLink>
        </nav>

        {/* Current User */}
        <div className="px-4 py-4 border-t border-gray-200">
          <div className="flex items-center gap-3 px-3 py-2.5 bg-gray-50 rounded-lg border border-gray-100">
            <div className="w-8 h-8 rounded-full bg-blue-500 flex items-center justify-center flex-shrink-0">
              {currentUser.display_name
                ? <span className="text-xs font-bold text-white uppercase">{currentUser.display_name[0]}</span>
                : <FiUser className="w-4 h-4 text-white" />
              }
            </div>
            <div className="min-w-0">
              {currentUser.email ? (
                <>
                  <p className="text-xs font-medium text-gray-900 truncate">{currentUser.display_name}</p>
                  <p className="text-xs text-gray-400 truncate">{currentUser.email}</p>
                </>
              ) : (
                <p className="text-xs text-gray-400">読み込み中...</p>
              )}
            </div>
          </div>
        </div>

        {/* Admin Link */}
        <div className="px-4 py-4 border-t border-gray-200">
          <NavLink
            to="/admin"
            className="flex items-center px-4 py-3 text-sm font-medium text-gray-600 hover:bg-gray-50 hover:text-gray-900 rounded-lg transition-colors"
          >
            <FiSettings className="w-5 h-5 mr-3" />
            管理者画面
          </NavLink>
        </div>

        {/* AI Status */}
        <div className="px-4 py-4 border-t border-gray-200">
          <div className="flex items-center px-4 py-3 bg-green-50 rounded-lg">
            <HiOutlineSparkles className="w-5 h-5 text-green-600" />
            <div className="ml-3">
              <p className="text-sm font-medium text-green-800">AI Ready</p>
              <p className="text-xs text-green-600">Claude Sonnet 4</p>
            </div>
          </div>
        </div>
      </aside>

      {/* Main Content */}
      <main className="flex-1 overflow-hidden">
        <Outlet />
      </main>
    </div>
  )
}
