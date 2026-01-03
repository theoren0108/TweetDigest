# 重新上传并设置主分支计划

您在 GitHub 上删除了 `main` 分支，但本地的 `main` 分支依然完好。我们只需要将其重新推送到远程即可。

## 1. 重新上传代码
我将执行以下 Git 命令：
1.  **推送分支**: `git push -u origin main`
    *   这将把本地的 `main` 分支上传到 GitHub，并重新建立关联。

## 2. 设置为主分支 (Default Branch)
由于我是通过命令行操作，无法直接修改 GitHub 的网页设置。推送完成后，您需要手动在 GitHub 网页上确认或设置：

1.  打开您的 GitHub 仓库页面 (`https://github.com/theoren0108/logseq`)。
2.  进入 **Settings** (设置) > **General**。
3.  找到 **Default branch** (默认分支) 区域。
4.  如果显示的不是 `main`，点击切换按钮（箭头图标），选择 `main` 并点击 **Update**。
    *   *注：如果仓库之前为空或这是唯一的活动分支，GitHub 通常会自动将其设为默认。*

准备好后，请批准计划，我将为您执行上传操作。