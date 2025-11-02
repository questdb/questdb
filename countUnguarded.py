class Solution(object):
    def countUnguarded(self, m, n, guards, walls):
        """
        :type m: int
        :type n: int
        :type guards: List[List[int]]
        :type walls: List[List[int]]
        :rtype: int
        """
        # 0 = empty, 1 = guard, 2 = wall, 3 = watched
        grid = [[0] * n for _ in range(m)]

        # Mark guards and walls
        for r, c in guards:
            grid[r][c] = 1
        for r, c in walls:
            grid[r][c] = 2

        # Directions: up, down, left, right
        directions = [(-1, 0), (1, 0), (0, -1), (0, 1)]

        # Mark all watched cells
        for r, c in guards:
            for dr, dc in directions:
                nr, nc = r + dr, c + dc
                while 0 <= nr < m and 0 <= nc < n and grid[nr][nc] not in (1, 2):
                    if grid[nr][nc] == 0:
                        grid[nr][nc] = 3  # mark as watched
                    nr += dr
                    nc += dc

        # Count unguarded cells (still 0)
        unguarded = sum(row.count(0) for row in grid)
        return unguarded


# Example usage
m = 4
n = 6
guards = [[0,0],[1,1],[2,3]]
walls = [[0,1],[2,2],[1,4]]

solution = Solution()
print(solution.countUnguarded(m, n, guards, walls))
