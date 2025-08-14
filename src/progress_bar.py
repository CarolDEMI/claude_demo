#!/usr/bin/env python3
"""
真实进度显示模块
"""
import time
import sys
from typing import Optional, Callable

class RealProgressBar:
    """真实进度条，根据实际任务进度更新"""
    
    def __init__(self, total: int, description: str = "Processing", width: int = 40):
        self.total = total
        self.current = 0
        self.description = description
        self.width = width
        self.start_time = time.time()
        self.last_update = 0
        
    def update(self, increment: int = 1, status: str = ""):
        """更新进度"""
        self.current = min(self.current + increment, self.total)
        
        # 避免过于频繁的更新
        if time.time() - self.last_update < 0.1 and self.current < self.total:
            return
        
        self.last_update = time.time()
        self._display(status)
    
    def set_progress(self, current: int, status: str = ""):
        """设置当前进度"""
        self.current = min(current, self.total)
        self._display(status)
    
    def _display(self, status: str = ""):
        """显示进度条"""
        if self.total == 0:
            return
            
        percentage = (self.current / self.total) * 100
        filled_width = int((self.current / self.total) * self.width)
        
        # 计算剩余时间
        elapsed = time.time() - self.start_time
        if self.current > 0:
            rate = self.current / elapsed
            if rate > 0:
                eta = (self.total - self.current) / rate
                eta_str = f" ETA: {self._format_time(eta)}"
            else:
                eta_str = ""
        else:
            eta_str = ""
        
        # 构建进度条
        bar = "█" * filled_width + "░" * (self.width - filled_width)
        
        # 状态信息
        status_str = f" | {status}" if status else ""
        
        # 完整的进度显示
        progress_line = f"\r{self.description}: [{bar}] {percentage:6.1f}% ({self.current}/{self.total}){eta_str}{status_str}"
        
        print(progress_line, end='', flush=True)
        
        # 完成时换行
        if self.current >= self.total:
            print()
    
    def _format_time(self, seconds: float) -> str:
        """格式化时间显示"""
        if seconds < 60:
            return f"{seconds:.0f}s"
        elif seconds < 3600:
            return f"{seconds/60:.0f}m{seconds%60:.0f}s"
        else:
            hours = int(seconds / 3600)
            minutes = int((seconds % 3600) / 60)
            return f"{hours}h{minutes}m"
    
    def finish(self, message: str = "完成"):
        """完成进度条"""
        self.current = self.total
        self._display(message)
        if not message.endswith('\n'):
            print()

class ProgressCallback:
    """进度回调类，用于在长时间操作中更新进度"""
    
    def __init__(self, progress_bar: RealProgressBar):
        self.progress_bar = progress_bar
        self.last_status = ""
    
    def __call__(self, current: int, status: str = ""):
        """更新进度的回调函数"""
        if status != self.last_status:
            self.progress_bar.set_progress(current, status)
            self.last_status = status
        else:
            self.progress_bar.set_progress(current)

def with_progress(total: int, description: str = "Processing"):
    """装饰器：为函数添加进度显示"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            progress_bar = RealProgressBar(total, description)
            
            # 将progress_callback传递给函数
            kwargs['progress_callback'] = ProgressCallback(progress_bar)
            
            try:
                result = func(*args, **kwargs)
                progress_bar.finish("✅ 完成")
                return result
            except Exception as e:
                progress_bar.finish("❌ 失败")
                raise e
        return wrapper
    return decorator

def progress_iterator(iterable, description: str = "Processing"):
    """为迭代器添加进度显示"""
    total = len(iterable) if hasattr(iterable, '__len__') else None
    
    if total is None:
        # 对于不知道长度的迭代器，使用简单的计数
        count = 0
        for item in iterable:
            count += 1
            print(f"\r{description}: {count} 项已处理...", end='', flush=True)
            yield item
        print(f"\n✅ 完成，共处理 {count} 项")
    else:
        progress_bar = RealProgressBar(total, description)
        for i, item in enumerate(iterable):
            progress_bar.update(1)
            yield item
        progress_bar.finish()

class MultiStepProgress:
    """多步骤进度显示"""
    
    def __init__(self, steps: list, description: str = "执行任务"):
        self.steps = steps
        self.total_steps = len(steps)
        self.current_step = 0
        self.description = description
        self.start_time = time.time()
    
    def start_step(self, step_index: int):
        """开始某个步骤"""
        self.current_step = step_index
        step_name = self.steps[step_index] if step_index < len(self.steps) else "未知步骤"
        percentage = ((step_index + 1) / self.total_steps) * 100
        
        print(f"\n🔄 步骤 {step_index + 1}/{self.total_steps}: {step_name}")
        print(f"📊 总进度: {percentage:.1f}%")
    
    def complete_step(self, step_index: int, success: bool = True):
        """完成某个步骤"""
        step_name = self.steps[step_index] if step_index < len(self.steps) else "未知步骤"
        status = "✅ 成功" if success else "❌ 失败"
        print(f"{status}: {step_name}")
        
        if step_index + 1 >= self.total_steps:
            elapsed = time.time() - self.start_time
            print(f"\n🎉 所有步骤完成！总耗时: {elapsed:.1f}秒")
    
    def __enter__(self):
        print(f"🚀 开始执行: {self.description}")
        print(f"📋 共 {self.total_steps} 个步骤")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            print(f"\n❌ 任务中断: {exc_val}")
        return False

# 实用函数
def show_fake_progress(duration: float = 3.0, description: str = "处理中"):
    """显示假进度条（用于演示或等待）"""
    steps = 50
    for i in range(steps + 1):
        percentage = (i / steps) * 100
        filled = "█" * int(i * 0.8)
        empty = "░" * int((steps - i) * 0.8)
        print(f"\r{description}: [{filled}{empty}] {percentage:6.1f}%", end='', flush=True)
        time.sleep(duration / steps)
    print("  ✓ 完成!")

if __name__ == '__main__':
    # 测试进度条
    print("🧪 测试进度显示功能...")
    
    # 测试1: 基本进度条
    print("\n1. 基本进度条测试:")
    progress = RealProgressBar(10, "下载文件")
    for i in range(11):
        progress.update(1, f"文件 {i+1}")
        time.sleep(0.3)
    
    # 测试2: 迭代器进度
    print("\n2. 迭代器进度测试:")
    data = list(range(20))
    for item in progress_iterator(data, "处理数据"):
        time.sleep(0.1)  # 模拟处理时间
    
    # 测试3: 多步骤进度
    print("\n3. 多步骤进度测试:")
    steps = ["初始化", "加载数据", "处理数据", "生成报告", "清理"]
    with MultiStepProgress(steps, "数据分析任务") as progress:
        for i, step in enumerate(steps):
            progress.start_step(i)
            time.sleep(1)  # 模拟工作
            progress.complete_step(i, True)
    
    print("\n✅ 所有测试完成！")